import pytest
import time
from pipeplan.core import Pipeline, ExecutionContext, task, TaskStatus
import concurrent.futures

@pytest.fixture
def fresh_context():
    """Provides a fresh, isolated ExecutionContext for every test."""
    return ExecutionContext()

# --- Dummy Functions for Testing ---
def return_one(): return 1
def add_one(x): return x + 1
def add_together(a, b): return a + b

def test_explicit_dag_resolution():
    """Tests if the core engine properly resolves dependencies using method chaining."""
    pipe = Pipeline("Test_DAG")
    
    # Build: A -> B, A -> C, (B, C) -> D
    pipe.add_task("task_a", func=return_one)
    pipe.add_task("task_b", func=add_one, depends_on={"x": "task_a"})
    pipe.add_task("task_c", func=add_one, depends_on={"x": "task_a"})
    pipe.add_task("task_d", func=add_together, depends_on={"a": "task_b", "b": "task_c"})
    
    pipe.run()
    
    assert pipe.nodes["task_a"].result.output == 1
    assert pipe.nodes["task_b"].result.output == 2
    assert pipe.nodes["task_c"].result.output == 2
    assert pipe.nodes["task_d"].result.output == 4
    assert pipe.nodes["task_d"].result.status == TaskStatus.COMPLETED

def test_implicit_dag_with_decorators():
    """Tests if the @task decorator correctly infers dependencies via Python variables."""
    @task
    def t_return_one(): return 1
    
    @task
    def t_add_one(x): return x + 1

    with Pipeline("Test_Decorators") as pipe:
        node_a = t_return_one()
        node_b = t_add_one(node_a)
        
    pipe.run()
    
    assert node_a.result.output == 1
    assert node_b.result.output == 2

def test_missing_dependency_fails_fast():
    """Tests pre-flight DAG validation. Should fail before executing anything."""
    pipe = Pipeline("Test_Missing_Dep")

    # Catch the error during the dynamic add_task mapping!
    with pytest.raises(ValueError, match="Parent task 'non_existent_task' not found"):
        pipe.add_task("task_a", func=return_one, depends_on={"x": "non_existent_task"})

    pipe.run()

def test_retry_mechanism_and_exponential_backoff():
    """Tests if a failing task correctly retries and respects backoff configuration."""
    attempts = 0
    
    # We use a tiny delay (0.01s) so the test runs instantly
    @task(retries=3, retry_delay=0.01)
    def flaky_task():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError("Simulated Failure")
        return "Success on 3"

    with Pipeline("Test_Retries") as pipe:
        node = flaky_task()
        
    pipe.run()
    
    assert attempts == 3
    assert node.result.output == "Success on 3"
    assert node.result.status == TaskStatus.COMPLETED

def test_reentrant_contexts(fresh_context):
    """Tests the stack-based Token logic to ensure contexts can be nested safely."""
    # The outer context
    with fresh_context:
        # A pipeline creates an inner context scope.
        with Pipeline("Reentrant_Pipe"):
            assert fresh_context._tokens  # Ensure a token is pushed
            
    # Once completely exited, the token stack should be empty
    assert len(fresh_context._tokens) == 0

def test_parallel_execution_speed():
    """Tests that independent tasks execute concurrently, saving time."""
    @task
    def slow_task():
        time.sleep(0.4)
        return True
    
    exec_factory = lambda: concurrent.futures.ThreadPoolExecutor(max_workers=4)

    with Pipeline("Parallel_Test", executor_factory=exec_factory) as pipe:
        # Register 4 tasks that do not depend on each other
        t1, t2, t3, t4 = slow_task(), slow_task(), slow_task(), slow_task()
        
    start_time = time.time()
    pipe.run()
    elapsed = time.time() - start_time
    
    # If run sequentially, it would take 0.16 seconds.
    # If parallelized properly, it should take ~0.4 seconds.
    assert elapsed < 0.6
    assert all([n.result.status == TaskStatus.COMPLETED for n in [t1, t2, t3, t4]])

def test_task_timeout_fail():
    @task
    def fast_task():
        time.sleep(0.05)
        return True

    @task
    def hung_task():
        time.sleep(10.0)  # Effectively infinite for test purposes
        return True

    @task
    def downstream_task(data):
        return data

    exec_factory = lambda: concurrent.futures.ThreadPoolExecutor(max_workers=2)
    
    with Pipeline("timeout_test", executor_factory=exec_factory) as pipe:
        node_a = fast_task()
        node_b = hung_task()  # No dependency on node_a — runs concurrently
        node_c = downstream_task(data=node_b)  # Should never run

    with pytest.raises(TimeoutError) as exc_info:
        pipe.run(task_timeout=0.5)

    assert "hung_task" in str(exc_info.value)