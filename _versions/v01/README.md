# PipePlan

A declarative, configuration-driven batch ETL framework on **pandas**. Pipelines
are described in modular YAML (or JSON) blueprints; the engine validates them
with **Pydantic v2**, builds a task DAG, and executes the data flow over an
in-memory dataframe state. The core stays pristine — every transform, expression,
notifier, and secret provider is resolved through a registry, extensible via
`pyproject.toml` entry points.

This is **version 1.0** — the canonical blueprint format is `apiVersion: pipeplan/v1`.

## Install

```bash
pip install -e ".[dev]"          # core + openpyxl, pyarrow, rapidfuzz, pytest
pip install -e ".[postgres]"     # psycopg2
pip install -e ".[access]"       # pyodbc + sqlalchemy-access (+ OS ODBC driver)
```

## Run the demo

```bash
python examples/demo/run_demo.py
```

It generates synthetic sources (a messy Excel workbook, a regions JSON, a SQLite
inventory DB), runs the modular blueprint under `examples/demo/pipeline/`, and
prints the resulting warehouse tables. It is idempotent — re-running reuses
transform checkpoints and produces identical output.

## CLI

```bash
pipeplan validate examples/demo/pipeline/pipeline.yaml --param run_date=2026-01-01
pipeplan run      examples/demo/pipeline/pipeline.yaml --param run_date=2026-01-01
pipeplan schema   > pipeplan.schema.json     # JSON Schema of the blueprint format
```

## Architecture

Control flow and data flow are strictly separated:

- **Orchestrator** builds the DAG from each task's `depends_on` plus the
  producer/consumer edges implied by `output`/`input`/`${pipe}` operands (so
  ordering is correct even when `depends_on` is omitted), detects cycles with
  `graphlib.TopologicalSorter`, and runs independent tasks concurrently up to
  `orchestration.max_parallelism`.
- **Engine** is the data flow: tasks read inputs from and write outputs to a
  thread-safe `Dict[str, pd.DataFrame]` state via explicit `input`/`output`.

Transforms are tiered: **element** (1:1 column ops), **set** (row/column masking
and reshaping), and **collection** (relational ops across frames). Collection ops
bind the dataframe flowing through a task with the explicit `${pipe}` token; a
missing operand is an error, never an implicit guess.

## Interpolation namespaces

Resolved at load time over the parsed structure (type-aware: a whole-value token
yields the raw value, so `"${var:regions}"` becomes a list):

| Token | Source |
|-------|--------|
| `${env:NAME}`    | process environment |
| `${var:name}`    | the `vars:` block |
| `${param:name}`  | a typed run parameter |
| `${secret:path}` | the secret provider (redacted in logs) |
| `${pipe}`        | runtime only — the frame flowing through a task |

## Schema contracts & expectations

A `schemas:` block declares per-dataframe contracts (dtype, nullable, unique,
allowed, checks, primary/foreign keys, strict). Tasks attach them via
`input_contract` / `output_contract` / a step `contract`, enforced per
`validate: off | warn | strict`. Softer `expectations` (row counts, null rates,
value bounds) can `warn` or `fail`.

## Transforms

`label, map, replace, cast, affix, normalize, derive, fillna` (element);
`filter, sort, dedupe, group, select, drop, window` (set);
`merge, join, union, fuzzy_join` (collection). `group` requires an explicit
`agg`. `cast` supports `on_error: quarantine` to divert un-parseable rows to a
`<output>__rejected` frame.

## Load modes

`replace` (optionally `write.partition_by`-scoped), `append`, `upsert`,
`delete`, and `scd2` (Type-2 history via `effective_from`/`effective_to`/
`current_flag`). All idempotent for a given key/partition.

## Incremental extract

`incremental: { strategy: watermark, cursor: <col>, lookback: 1d, initial: ... }`
reads only rows whose cursor advanced since the persisted high-water mark
(stored in `orchestration.watermark_store`), with a safety lookback window.

## Extending

```toml
[project.entry-points."pipeplan.transforms"]
my_action = "my_pkg.module:MyTransform"

[project.entry-points."pipeplan.notifiers"]
slack = "my_pkg.module:make_slack_notifier"
```

## Tests

```bash
pytest -q
```

See `MIGRATION.md` for the mapping from the pre-1.0 JSON format to v1.
