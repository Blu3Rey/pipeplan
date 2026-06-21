import time
import uuid
import logging
import concurrent.futures
from typing import Dict, Callable, Any, List, Optional, Protocol, runtime_checkable, Set, overload, Union
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from graphlib import TopologicalSorter, CycleError
import contextvars
from contextvars import ContextVar

log = logging.getLogger(__name__)

# ==========================================
# Thread-Safe Global Contexts
# ==========================================
current_context: ContextVar["ExecutionContext"] = ContextVar("current_context")
current_pipeline: ContextVar["Pipeline"] = ContextVar("current_pipeline", default=None)

@runtime_checkable
class ResourceProtocol(Protocol):
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def read(self, *args, **kwargs) -> Any: ...
    def write(self, data: Any, *args, **kwargs) -> None: ...
    def __enter__(self) -> "ResourceProtocol": ...
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...

class ExecutionContext:
    def __init__(self):
        self._connections: Dict[str, ResourceProtocol] = {}
        # Use a list to support nested (re-entrant) context managers
        self._tokens: List[contextvars.Token] = []
        
    def register_resource(self, id: str, resource: ResourceProtocol):
        # self._connections[id] = resource
        if not isinstance(resource, ResourceProtocol):
            raise TypeError(
                f"Resource '{id}' does not implement ResourceProtocol. "
                f"Missing methods: connect, disconnect, read, write."
            )
        self._connections[id] = resource
        
    def get_resource(self, id: str) -> ResourceProtocol:
        if id not in self._connections:
            raise KeyError(f"Unknown resource connection: {id}")
        return self._connections[id]

    def close_all(self):
        for res_id, conn in self._connections.items():
            try:
                conn.disconnect()
            except Exception as e:
                log.warning(f"Failed to close resource '{res_id}': {e}")
        self._connections.clear()

    def __enter__(self):
        # Push the new token onto the stack
        self._tokens.append(current_context.set(self))
        return self

    def __exit__(self, exc_type, exc, tb):
        token = self._tokens.pop() if self._tokens else None
        if token:
            current_context.reset(token)
        if not self._tokens:
            self.close_all()


# ==========================================
# Task State & Nodes
# ==========================================
class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class TaskResult:
    status: TaskStatus = TaskStatus.PENDING
    output: Any = None
    error: Optional[Exception] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    elapsed_time: float = 0.0

@dataclass
class TaskNode:
    id: str
    func: Callable
    args: tuple
    kwargs: dict
    retries: int
    retry_delay: float
    result: TaskResult = field(default_factory=TaskResult)


# ==========================================
# The Declarative @task Decorator
# ==========================================
def task(func=None, *, retries: int = 3, retry_delay: float = 1.0):
    """
    Decorator that tracks function calls inside a Pipeline context,
    automatically building the dependency DAG.
    """
    def decorator(fn: Callable):
        @wraps(fn)
        def wrapper(*args, **kwargs) -> Union[Any, TaskNode]:
            pipe = current_pipeline.get()
            
            # If called outside a `with Pipeline():` block, execute eagerly
            if pipe is None:
                return fn(*args, **kwargs)
            
            # If inside a pipeline, register it as a node (Lazy execution)
            node_id = f"{fn.__name__}_{uuid.uuid4().hex[:6]}"
            node = TaskNode(
                id=node_id, func=fn, args=args, kwargs=kwargs,
                retries=retries, retry_delay=retry_delay
            )
            pipe.add_node(node)
            return node 
            
        return wrapper
    if func is None: return decorator
    return decorator(func)


# ==========================================
# Pipeline Engine (Parallel Execution)
# ==========================================
class Pipeline:
    def __init__(
        self,
        id: str,
        context: Optional[ExecutionContext] = None,
        executor_factory: Callable = None
    ):
        self.id = id
        self.nodes: Dict[str, TaskNode] = {}
        
        # Auto-detect the active context!
        self.context = context or current_context.get(None) or ExecutionContext()

        # Make the pipeline  executor configurable
        self.executor_factory = executor_factory or (
            lambda: concurrent.futures.ThreadPoolExecutor(max_workers=4)
        )
        
        # Make the pipeline re-entrant as well
        self._tokens: List[contextvars.Token] = []

    def __enter__(self):
        self._tokens.append(current_pipeline.set(self))
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._tokens:
            current_pipeline.reset(self._tokens.pop())

    def add_node(self, node: TaskNode) -> "Pipeline":
        self.nodes[node.id] = node
        return self

    def add_task(
        self, 
        id: str, 
        func: Callable, 
        args: tuple = None, 
        kwargs: dict = None, 
        depends_on: dict = None, 
        retries: int = 3, 
        retry_delay: float = 1.0
    ) -> "Pipeline":
        """Method Chaining API for programmatic DAG construction."""
        args = args or ()
        kwargs = dict(kwargs or {})  
        depends_on = depends_on or {}

        for kwarg_name, parent_id in depends_on.items():
            if parent_id not in self.nodes:
                raise ValueError(f"Parent task '{parent_id}' not found.")
            kwargs[kwarg_name] = self.nodes[parent_id]

        node = TaskNode(
            id=id, func=func, args=args, kwargs=kwargs, 
            retries=retries, retry_delay=retry_delay
        )
        self.nodes[id] = node
        return self

    def _extract_dependencies(self, val: Any, deps: Set[str]):
        if isinstance(val, TaskNode):
            deps.add(val.id)
        elif isinstance(val, (list, tuple)):
            for v in val: self._extract_dependencies(v, deps)
        elif isinstance(val, dict):
            for v in val.values(): self._extract_dependencies(v, deps)

    def _resolve_args(self, val: Any) -> Any:
        if isinstance(val, TaskNode):
            if val.result.status != TaskStatus.COMPLETED:
                raise RuntimeError(f"Dependency {val.id} failed or was not run.")
            return val.result.output
        elif isinstance(val, list):
            return [self._resolve_args(v) for v in val]
        elif isinstance(val, tuple):
            return tuple(self._resolve_args(v) for v in val)
        elif isinstance(val, dict):
            return {k: self._resolve_args(v) for k, v in val.items()}
        return val

    def _execute_node(self, node: TaskNode):
        """Encapsulated execution logic for thread workers with exponential backoff."""
        node.result.start_time = time.time()
        node.result.status = TaskStatus.RUNNING
        
        real_args = self._resolve_args(node.args)
        real_kwargs = self._resolve_args(node.kwargs)
        
        remaining_attempts = node.retries
        current_delay = node.retry_delay
        backoff_factor = 2.0

        while remaining_attempts > 0:
            try:
                if node.retries > 1:
                    log.info(f"Running Task: {node.id} (Attempt {node.retries - remaining_attempts + 1}/{node.retries})")
                else:
                    log.info(f"Running Task: {node.id}")
                    
                node.result.output = node.func(*real_args, **real_kwargs)
                node.result.status = TaskStatus.COMPLETED
                break
            except Exception as e:
                remaining_attempts -= 1
                if remaining_attempts <= 0:
                    node.result.status = TaskStatus.FAILED
                    node.result.error = e
                    log.error(f"Task {node.id} failed permanently: {e}")
                    
                    node.result.end_time = time.time()
                    node.result.elapsed_time = round(node.result.end_time - node.result.start_time, 4)
                    raise
                else:
                    log.warning(f"Task {node.id} failed. Retrying in {current_delay}s. Error: {e}")
                    time.sleep(current_delay)
                    current_delay *= backoff_factor

        node.result.end_time = time.time()
        node.result.elapsed_time = round(node.result.end_time - node.result.start_time, 4)

    def run(self, task_timeout: float = None):
        """Executes tasks topologically, utilizing parallel threads."""
        log.info(f"Executing Pipeline: {self.id}")
        
        # Reset state to allow safe re-runs
        for node in self.nodes.values():
            node.result = TaskResult()
            
        dag: Dict[str, Set[str]] = {}
        all_deps = set()
        
        for node_id, node in self.nodes.items():
            deps = set()
            self._extract_dependencies(node.args, deps)
            self._extract_dependencies(node.kwargs, deps)
            dag[node_id] = deps
            all_deps.update(deps)

        missing_deps = all_deps - set(self.nodes.keys())
        if missing_deps:
            raise ValueError(f"Pipeline validation failed: Missing required dependency nodes {missing_deps}")

        ts = TopologicalSorter(dag)
        try:
            ts.prepare()
        except CycleError as e:
            log.critical(f"Cyclic dependency detected: {e}")
            raise

        with self.context:
            with self.executor_factory() as executor:
                running_tasks = {}
                
                while ts.is_active():                    
                    for node_id in ts.get_ready():
                        node = self.nodes[node_id]

                        # 1. Capture the context from the main thread
                        ctx = contextvars.copy_context()
                        
                        # 2. Use ctx.run to inject the main thread's context into the worker thread!
                        future = executor.submit(ctx.run, self._execute_node, node)
                        running_tasks[future] = node_id
                    
                    if not running_tasks:
                        break
                        
                    done, not_done = concurrent.futures.wait(
                        running_tasks.keys(),
                        timeout=task_timeout,
                        return_when=concurrent.futures.FIRST_COMPLETED
                    )

                    # Handle timeout
                    if not_done and task_timeout is not None and not done:
                        for future in not_done:
                            future.cancel()
                        timed_out_ids = [running_tasks[f] for f in not_done]
                        raise TimeoutError(f"Pipeline '{self.id}' timed out waiting on: {timed_out_ids}")
                    
                    # Handle completed futures
                    for future in done:
                        node_id = running_tasks.pop(future)
                        try:
                            future.result()
                            ts.done(node_id)
                        except Exception as e:
                            # Cancel all pending futures
                            for pending in running_tasks:
                                pending.cancel()
                            raise RuntimeError(f"Pipeline '{self.id}' aborted: task '{node_id}' failed: {e}") from e
                        
        log.info(f"Pipeline '{self.id}' execution finished.")