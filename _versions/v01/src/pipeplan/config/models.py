"""Pydantic v2 schema for the declarative pipeline configuration (v1).

This is the canonical PipePlan blueprint format: ``apiVersion: pipeplan/v1``,
authored in YAML (or JSON), assembled from modular fragments, and validated here
before any data is touched. Tasks form a discriminated union keyed on ``stage``.

The loader applies ``imports``, ``defaults`` and ``templates`` *before* a
document reaches this model, so by validation time every task is fully resolved.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator

from ..core.contracts import DataframeContract, Expectation

# Operand sentinel for the explicit piped-data rule. Absence of a `left` operand
# is now an error; a collection op must say ${pipe} to bind the flowing frame.
PIPE_TOKENS: frozenset[str] = frozenset({"${pipe}", "$pipe"})


# --------------------------------------------------------------------------- #
# Enumerations
# --------------------------------------------------------------------------- #


class Stage(str, Enum):
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"


class AdapterKind(str, Enum):
    FILE = "file"
    DB = "db"


class Permission(str, Enum):
    READ = "read"
    WRITE = "write"


class LoadMode(str, Enum):
    REPLACE = "replace"   # idempotent: target ends up exactly equal to the frame
    APPEND = "append"
    UPSERT = "upsert"     # idempotent on `key`: delete matching keys, then insert
    SCD2 = "scd2"         # Type-2 history: close changed rows, insert new versions
    DELETE = "delete"     # remove rows matching the frame's keys


class OnFailure(str, Enum):
    FAIL = "fail"
    SKIP = "skip"
    CONTINUE = "continue"
    FALLBACK = "fallback"


class ValidationMode(str, Enum):
    OFF = "off"
    WARN = "warn"
    STRICT = "strict"


class IncrementalStrategy(str, Enum):
    FULL = "full"
    WATERMARK = "watermark"
    CDC = "cdc"


# --------------------------------------------------------------------------- #
# Shared building blocks
# --------------------------------------------------------------------------- #


class StrictModel(BaseModel):
    """Base model: forbid unknown keys, accept field names or aliases."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, use_enum_values=False)


class RetryPolicy(StrictModel):
    attempts: int = Field(default=1, ge=1)
    delay: str | float = Field(default=0.0, description="Seconds, or a duration like '30s'.")
    backoff: float = Field(default=1.0, ge=1.0)
    timeout: str | float | None = Field(default=None, description="Per-attempt timeout, e.g. '10m'.")


class ResourceConfig(StrictModel):
    """A named external system (a file path or a database connection)."""

    name: str | None = None
    adapter: AdapterKind
    params: dict[str, Any]
    allow: list[Permission] = Field(default_factory=lambda: [Permission.READ, Permission.WRITE])

    def permits(self, permission: Permission) -> bool:
        return permission in self.allow


class ParameterSpec(StrictModel):
    type: Literal["string", "integer", "float", "boolean", "date", "datetime"] = "string"
    required: bool = False
    default: Any = None
    allowed: list[Any] | None = None
    description: str | None = None


class Settings(StrictModel):
    timezone: str = "UTC"
    engine: Literal["pandas"] = "pandas"


# --- partitioning ----------------------------------------------------------- #


class PartitionBounds(StrictModel):
    start: str | None = None
    end: str | None = None
    lookback: str | None = None  # duration applied as a safety re-read window


class Backfill(StrictModel):
    enabled: bool = False
    max_partitions: int = Field(default=365, ge=1)


class PartitionConfig(StrictModel):
    field: str
    granularity: Literal["hour", "day", "week", "month"] = "day"
    bounds: PartitionBounds = Field(default_factory=PartitionBounds)
    backfill: Backfill = Field(default_factory=Backfill)


# --- orchestration ---------------------------------------------------------- #


class CheckpointConfig(StrictModel):
    enabled: bool = False
    store: str | None = None
    reuse: bool = True


class WatermarkStoreConfig(StrictModel):
    resource: str
    table: str = "pipeplan_watermarks"


class NotificationChannel(StrictModel):
    type: str = "log"
    target: str | None = None


class Notifications(StrictModel):
    on_failure: list[NotificationChannel] = Field(default_factory=list)
    on_sla: list[NotificationChannel] = Field(default_factory=list)


class Sla(StrictModel):
    max_runtime: str | None = None
    warn_after: str | None = None


class Orchestration(StrictModel):
    max_parallelism: int = Field(default=1, ge=1)
    fail_fast: bool = True
    checkpoint: CheckpointConfig = Field(default_factory=CheckpointConfig)
    watermark_store: WatermarkStoreConfig | None = None
    notifications: Notifications = Field(default_factory=Notifications)
    sla: Sla = Field(default_factory=Sla)


class TaskDefaults(StrictModel):
    retry: RetryPolicy | None = None
    on_failure: OnFailure = OnFailure.FAIL
    checkpoint: bool = True
    validation: ValidationMode = Field(default=ValidationMode.STRICT, alias="validate")


# --------------------------------------------------------------------------- #
# Steps
# --------------------------------------------------------------------------- #


class ExtractStep(StrictModel):
    collection: str | None = None  # None for single-collection sources
    output: str
    contract: str | None = None    # schema name to validate immediately on read


class IncrementalConfig(StrictModel):
    strategy: IncrementalStrategy = IncrementalStrategy.FULL
    cursor: str | None = None
    lookback: str | None = None
    initial: Any = None

    @model_validator(mode="after")
    def _watermark_needs_cursor(self) -> "IncrementalConfig":
        if self.strategy is IncrementalStrategy.WATERMARK and not self.cursor:
            raise ValueError("incremental strategy 'watermark' requires a 'cursor' column")
        return self


class TransformStep(StrictModel):
    id: str | None = None
    description: str | None = None
    action: str
    with_: dict[str, Any] | list[Any] = Field(default_factory=dict, alias="with")
    on_error: Literal["fail", "skip", "quarantine"] = "fail"


class WriteSpec(StrictModel):
    partition_by: list[str] = Field(default_factory=list)
    chunksize: int | None = Field(default=10_000, ge=1)


class ScdSpec(StrictModel):
    track: list[str] = Field(min_length=1)
    effective_from: str = "valid_from"
    effective_to: str = "valid_to"
    current_flag: str = "is_current"


class LoadStep(StrictModel):
    input: str
    collection: str
    mode: LoadMode = LoadMode.REPLACE
    key: str | list[str] | None = None
    input_contract: str | None = None
    write: WriteSpec = Field(default_factory=WriteSpec)
    scd: ScdSpec | None = None

    @model_validator(mode="after")
    def _mode_requirements(self) -> "LoadStep":
        if self.mode in (LoadMode.UPSERT, LoadMode.SCD2, LoadMode.DELETE) and not self.key:
            raise ValueError(f"load mode '{self.mode.value}' requires a 'key'")
        if self.mode is LoadMode.SCD2 and self.scd is None:
            raise ValueError("load mode 'scd2' requires an 'scd' block (track columns)")
        if self.mode is not LoadMode.SCD2 and self.scd is not None:
            raise ValueError("'scd' block is only valid with mode 'scd2'")
        return self


# --------------------------------------------------------------------------- #
# Tasks (discriminated on `stage`)
# --------------------------------------------------------------------------- #


class BaseTask(StrictModel):
    name: str | None = None
    depends_on: list[str] = Field(default_factory=list)
    retry: RetryPolicy | None = None
    on_failure: OnFailure = OnFailure.FAIL
    checkpoint: bool = True
    validation: ValidationMode = Field(default=ValidationMode.STRICT, alias="validate")


class ExtractTask(BaseTask):
    stage: Literal[Stage.EXTRACT] = Stage.EXTRACT
    resource: str
    incremental: IncrementalConfig | None = None
    steps: list[ExtractStep] = Field(min_length=1)


class TransformTask(BaseTask):
    stage: Literal[Stage.TRANSFORM] = Stage.TRANSFORM
    input: str | None = None
    output: str
    input_contract: str | None = None
    output_contract: str | None = None
    expectations: list[Expectation] = Field(default_factory=list)
    steps: list[TransformStep] = Field(min_length=1)


class LoadTask(BaseTask):
    stage: Literal[Stage.LOAD] = Stage.LOAD
    resource: str
    steps: list[LoadStep] = Field(min_length=1)


Task = Annotated[
    Union[ExtractTask, TransformTask, LoadTask],
    Field(discriminator="stage"),
]


# --------------------------------------------------------------------------- #
# Top-level pipeline
# --------------------------------------------------------------------------- #


class PipelineConfig(StrictModel):
    apiVersion: str = "pipeplan/v1"
    kind: Literal["Pipeline"] = "Pipeline"
    metadata: dict[str, Any] = Field(default_factory=dict)
    settings: Settings = Field(default_factory=Settings)
    parameters: dict[str, ParameterSpec] = Field(default_factory=dict)
    partition: PartitionConfig | None = None
    defaults: TaskDefaults = Field(default_factory=TaskDefaults)
    templates: dict[str, dict[str, Any]] = Field(default_factory=dict)
    orchestration: Orchestration = Field(default_factory=Orchestration)
    vars: dict[str, Any] = Field(default_factory=dict)
    resources: dict[str, ResourceConfig] = Field(default_factory=dict)
    schemas: dict[str, DataframeContract] = Field(default_factory=dict)
    tasks: dict[str, Task]

    # Runtime extras populated by the loader (not part of the blueprint) ---- #
    _runtime_params: dict[str, Any] = PrivateAttr(default_factory=dict)
    _secret_values: set[str] = PrivateAttr(default_factory=set)

    @property
    def runtime_params(self) -> dict[str, Any]:
        return self._runtime_params

    @property
    def secret_values(self) -> set[str]:
        return self._secret_values

    # Convenience accessors used throughout the codebase ------------------- #

    @property
    def pipeline_id(self) -> str:
        return str(self.metadata.get("id", "pipeline"))

    @property
    def timezone(self) -> str:
        return self.settings.timezone

    # -- name injection ---------------------------------------------------- #

    @model_validator(mode="before")
    @classmethod
    def _inject_names(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        for section in ("resources", "tasks", "schemas"):
            block = data.get(section)
            if isinstance(block, dict):
                for key, value in block.items():
                    if isinstance(value, dict):
                        value.setdefault("name", key)
        return data

    # -- cross-reference integrity ---------------------------------------- #

    @model_validator(mode="after")
    def _validate_references(self) -> "PipelineConfig":
        self._check_resources_and_deps()
        self._check_contracts_exist()
        self._check_unique_producers()
        self._check_consumed_produced()
        self._check_pipe_usage()
        return self

    def _check_resources_and_deps(self) -> None:
        for task in self.tasks.values():
            res = getattr(task, "resource", None)
            if res is not None and res not in self.resources:
                raise ValueError(f"task '{task.name}' references unknown resource '{res}'")
            for dep in task.depends_on:
                if dep not in self.tasks:
                    raise ValueError(f"task '{task.name}' depends on unknown task '{dep}'")

    def _check_contracts_exist(self) -> None:
        def _require(ref: str | None, where: str) -> None:
            if ref is not None and ref not in self.schemas:
                raise ValueError(f"{where} references unknown schema '{ref}'")

        for task in self.tasks.values():
            if isinstance(task, TransformTask):
                _require(task.input_contract, f"task '{task.name}' input_contract")
                _require(task.output_contract, f"task '{task.name}' output_contract")
            elif isinstance(task, ExtractTask):
                for step in task.steps:
                    _require(step.contract, f"task '{task.name}' step '{step.output}'")
            elif isinstance(task, LoadTask):
                for step in task.steps:
                    _require(step.input_contract, f"task '{task.name}' step '{step.collection}'")

    def _check_unique_producers(self) -> None:
        producers: dict[str, str] = {}
        for name, task in self.tasks.items():
            for produced in _task_outputs(task):
                if produced in producers:
                    raise ValueError(
                        f"dataframe '{produced}' is produced by both "
                        f"'{producers[produced]}' and '{name}' (outputs must be unique)"
                    )
                producers[produced] = name

    def _check_consumed_produced(self) -> None:
        produced = self.produced_dataframes()
        for name, consumed in self.consumed_dataframes().items():
            missing = consumed - produced
            if missing:
                raise ValueError(
                    f"task '{name}' consumes dataframe(s) {sorted(missing)} "
                    f"that no task produces"
                )

    def _check_pipe_usage(self) -> None:
        for name, task in self.tasks.items():
            if not isinstance(task, TransformTask):
                continue
            for step in task.steps:
                if step.action == "union":
                    frames = step.with_.get("frames") if isinstance(step.with_, dict) else None
                    if not isinstance(frames, list) or not frames:
                        raise ValueError(
                            f"task '{name}': union requires a non-empty 'frames' list"
                        )

    # -- dataframe graph helpers ------------------------------------------ #

    def produced_dataframes(self) -> set[str]:
        produced: set[str] = set()
        for task in self.tasks.values():
            produced.update(_task_outputs(task))
        return produced

    def consumed_dataframes(self) -> dict[str, set[str]]:
        consumed: dict[str, set[str]] = {}
        for task in self.tasks.values():
            names = _task_inputs(task)
            if names:
                consumed[task.name] = names
        return consumed


# --------------------------------------------------------------------------- #
# Free functions over tasks (shared by the model + orchestrator)
# --------------------------------------------------------------------------- #


def _task_outputs(task: Task) -> set[str]:
    if isinstance(task, ExtractTask):
        return {step.output for step in task.steps}
    if isinstance(task, TransformTask):
        return {task.output}
    return set()


def _step_operands(step: TransformStep) -> set[str]:
    """Dataframe names a transform step reads from the shared state."""
    names: set[str] = set()
    with_ = step.with_
    if not isinstance(with_, dict):
        return names
    for key in ("left", "right"):
        ref = with_.get(key)
        if isinstance(ref, str) and ref not in PIPE_TOKENS:
            names.add(ref)
    frames = with_.get("frames")
    if isinstance(frames, list):
        names.update(f for f in frames if isinstance(f, str) and f not in PIPE_TOKENS)
    return names


def _task_inputs(task: Task) -> set[str]:
    names: set[str] = set()
    if isinstance(task, TransformTask):
        if task.input:
            names.add(task.input)
        for step in task.steps:
            names.update(_step_operands(step))
    elif isinstance(task, LoadTask):
        names.update(step.input for step in task.steps)
    return names
