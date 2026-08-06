"""The orchestrator: control flow over the task DAG.

Strictly separated from data flow. Responsibilities:

* build the DAG from each task's ``depends_on`` plus producer/consumer edges
  derived from ``output``/``input``/``${pipe}`` operands, so ordering is correct
  even when ``depends_on`` is omitted (data edges are always added);
* topologically schedule with :class:`graphlib.TopologicalSorter`, executing
  independent tasks concurrently up to ``orchestration.max_parallelism``;
* lazily provision only the adapters a run touches;
* wire the watermark store, checkpoint store (resume), notifications and SLA;
* honour per-task ``on_failure`` and the global ``fail_fast`` policy.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from graphlib import CycleError, TopologicalSorter

from .adapters.factory import create_adapter
from .config.models import (
    ExtractTask,
    LoadTask,
    OnFailure,
    PipelineConfig,
    TransformTask,
    _task_inputs,
    _task_outputs,
)
from .core.checkpoint import CheckpointStore, frame_fingerprint
from .core.context import ExecutionContext
from .core.durations import parse_seconds
from .core.exceptions import DependencyError, PipePlanError
from .core.notify import build_notifier
from .core.watermark import WatermarkStore
from .tasks.runner import run_extract, run_load, run_transform

logger = logging.getLogger("pipeplan")


class Orchestrator:
    """Execute a validated :class:`PipelineConfig` end to end."""

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.context = ExecutionContext(config=config)
        self._producer: dict[str, str] = {}
        for name, task in config.tasks.items():
            for produced in _task_outputs(task):
                self._producer[produced] = name

    # ------------------------------------------------------------------ #
    # graph construction
    # ------------------------------------------------------------------ #

    def _predecessors(self, name: str) -> set[str]:
        task = self.config.tasks[name]
        preds: set[str] = set(task.depends_on)
        for df_name in _task_inputs(task):
            src = self._producer.get(df_name)
            if src and src != name:
                preds.add(src)
        return preds

    def _build_graph(self) -> TopologicalSorter[str]:
        sorter: TopologicalSorter[str] = TopologicalSorter()
        for name in self.config.tasks:
            sorter.add(name, *self._predecessors(name))
        return sorter

    def _check_acyclic(self) -> None:
        try:
            list(self._build_graph().static_order())
        except CycleError as exc:
            cycle = exc.args[1] if len(exc.args) > 1 else exc.args
            raise DependencyError(f"task dependency graph contains a cycle: {cycle}") from exc

    def execution_order(self) -> list[str]:
        """Return one valid topological order (for validation / inspection)."""
        try:
            return list(self._build_graph().static_order())
        except CycleError as exc:
            cycle = exc.args[1] if len(exc.args) > 1 else exc.args
            raise DependencyError(f"task dependency graph contains a cycle: {cycle}") from exc

    # ------------------------------------------------------------------ #
    # provisioning & runtime stores
    # ------------------------------------------------------------------ #

    def _provision_adapters(self) -> None:
        needed = {
            getattr(task, "resource", None)
            for task in self.config.tasks.values()
            if getattr(task, "resource", None)
        }
        wm = self.config.orchestration.watermark_store
        if wm is not None:
            needed.add(wm.resource)
        for res_name in needed:
            if res_name and res_name not in self.context.adapters:
                self.context.adapters[res_name] = create_adapter(self.config.resources[res_name])

    def _setup_runtime(self) -> None:
        cfg = self.config
        ctx = self.context
        ctx.params = cfg.runtime_params
        ctx.secret_values = cfg.secret_values
        ctx.run_id = f"{cfg.pipeline_id}-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
        if cfg.partition is not None:
            ctx.partition_value = cfg.partition.bounds.start
        ctx.notifier = build_notifier(
            [c.model_dump() for c in cfg.orchestration.notifications.on_failure]
        )
        wm = cfg.orchestration.watermark_store
        if wm is not None:
            ctx.watermark_store = WatermarkStore(self.context.adapters[wm.resource], wm.table)
        ckpt = cfg.orchestration.checkpoint
        if ckpt.enabled and ckpt.store:
            ctx.checkpoint_store = CheckpointStore(ckpt.store, reuse=ckpt.reuse)

    # ------------------------------------------------------------------ #
    # execution
    # ------------------------------------------------------------------ #

    def run(self) -> ExecutionContext:
        self._check_acyclic()
        self._provision_adapters()
        self._setup_runtime()
        logger.info("pipeline '%s': %d task(s)", self.config.pipeline_id, len(self.config.tasks))
        start = time.monotonic()
        try:
            self._execute()
        finally:
            self._dispose_adapters()
        self._check_sla(time.monotonic() - start)
        return self.context

    def _execute(self) -> None:
        sorter = self._build_graph()
        sorter.prepare()
        max_par = max(1, self.config.orchestration.max_parallelism)
        fail_fast = self.config.orchestration.fail_fast
        errors: list[tuple[str, Exception]] = []
        abort = False

        with ThreadPoolExecutor(max_workers=max_par) as pool:
            in_flight: dict[Future, str] = {}
            while sorter.is_active():
                if not abort:
                    for name in sorter.get_ready():
                        in_flight[pool.submit(self._dispatch, name)] = name
                if not in_flight:
                    break
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for fut in done:
                    name = in_flight.pop(fut)
                    exc = fut.exception()
                    if exc is None:
                        sorter.done(name)
                        continue
                    policy = self.config.tasks[name].on_failure
                    if policy is OnFailure.FAIL:
                        errors.append((name, exc))
                        logger.error("task '%s' failed: %s", name, exc)
                        if fail_fast:
                            abort = True
                        else:
                            sorter.done(name)
                    else:  # skip / continue / fallback -> log and proceed
                        logger.warning("task '%s' failed but on_failure=%s; continuing: %s",
                                       name, policy.value, exc)
                        sorter.done(name)
                if abort and not in_flight:
                    break

        if errors:
            first_name, first_exc = errors[0]
            if self.context.notifier is not None:
                self.context.notifier.notify(
                    "failure", f"pipeline '{self.config.pipeline_id}' task '{first_name}': {first_exc}"
                )
            raise first_exc

    def _dispatch(self, name: str) -> None:
        task = self.config.tasks[name]
        logger.info("running task '%s' (%s)", name, task.stage.value)
        if isinstance(task, ExtractTask):
            run_extract(task, self.context)
        elif isinstance(task, TransformTask):
            self._dispatch_transform(task)
        elif isinstance(task, LoadTask):
            run_load(task, self.context)
        else:  # pragma: no cover - guarded by the discriminated union
            raise PipePlanError(f"unknown task type for '{name}'")

    def _dispatch_transform(self, task: TransformTask) -> None:
        store = self.context.checkpoint_store
        if store is None or not task.checkpoint:
            run_transform(task, self.context)
            return
        inputs = sorted(_task_inputs(task))
        fps = [frame_fingerprint(self.context.state.get(n, copy=False))
               for n in inputs if self.context.state.has(n)]
        key = store.key(task.name, fps)
        cached = store.load(key)
        if cached is not None:
            self.context.state.set(task.output, cached)
            return
        run_transform(task, self.context)
        store.save(key, self.context.state.get(task.output, copy=False))

    def _check_sla(self, elapsed: float) -> None:
        sla = self.config.orchestration.sla
        if sla.max_runtime and elapsed > parse_seconds(sla.max_runtime):
            msg = f"pipeline '{self.config.pipeline_id}' exceeded SLA {sla.max_runtime} ({elapsed:.1f}s)"
            logger.warning(msg)
            for chan in self.config.orchestration.notifications.on_sla:
                if self.context.notifier is not None:
                    self.context.notifier.notify("sla", msg, chan.target)
        elif sla.warn_after and elapsed > parse_seconds(sla.warn_after):
            logger.warning("pipeline '%s' ran %.1fs (warn_after %s)",
                           self.config.pipeline_id, elapsed, sla.warn_after)

    def _dispose_adapters(self) -> None:
        for adapter in self.context.adapters.values():
            dispose = getattr(adapter, "dispose", None)
            if callable(dispose):
                try:
                    dispose()
                except Exception:  # pragma: no cover - best-effort cleanup
                    logger.warning("failed to dispose adapter '%s'", adapter.name)


def run_pipeline(config: PipelineConfig) -> ExecutionContext:
    """Convenience wrapper: orchestrate ``config`` and return the final context."""
    return Orchestrator(config).run()
