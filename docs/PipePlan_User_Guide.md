# PipePlan — User's Guide

**A declarative, configuration-driven batch ETL framework on pandas.**
Blueprint format: `apiVersion: pipeplan/v1` · Package version: `1.0.0`

PipePlan describes an entire batch ETL pipeline as a set of modular YAML (or JSON)
blueprints. The engine validates every blueprint with **Pydantic v2**, builds a
task **DAG**, and executes tiered, fully **vectorised** pandas transformations
against an in-memory dataframe state. The core is deliberately pristine: every
transform, expression, notifier, and secret provider is resolved through a
registry and is extensible via `pyproject.toml` entry points — the engine never
hard-codes a list of verbs.

```
blueprint.yaml ─▶ imports + deep-merge ─▶ params + interpolation ─▶ Pydantic validation
                ─▶ DAG (graphlib) ─▶ tiered vectorised transforms ─▶ adapters
```

---

## Table of contents

1. [Design philosophy](#1-design-philosophy)
2. [Installation](#2-installation)
3. [Quick start](#3-quick-start)
4. [Blueprint anatomy](#4-blueprint-anatomy)
5. [Modular configuration: imports, templates, defaults](#5-modular-configuration)
6. [Interpolation namespaces](#6-interpolation-namespaces)
7. [Run parameters](#7-run-parameters)
8. [Resources and adapters](#8-resources-and-adapters)
9. [Tasks and steps](#9-tasks-and-steps)
10. [The `${pipe}` rule (data flow inside a task)](#10-the-pipe-rule)
11. [Transform catalogue](#11-transform-catalogue)
    - [Element tier](#element-tier)
    - [Set tier](#set-tier)
    - [Collection tier](#collection-tier)
12. [The AST subsystems: filter & compute](#12-the-ast-subsystems)
13. [Schema contracts and expectations](#13-schema-contracts-and-expectations)
14. [Loading: write modes](#14-loading-write-modes)
15. [Incremental extract and watermarks](#15-incremental-extract-and-watermarks)
16. [Orchestration](#16-orchestration)
17. [Secrets and notifiers](#17-secrets-and-notifiers)
18. [Command-line interface](#18-command-line-interface)
19. [Extending PipePlan](#19-extending-pipeplan)
20. [Migrating from the pre-1.0 JSON format](#20-migrating-from-the-pre-10-json-format)
21. [End-to-end worked example](#21-end-to-end-worked-example)
22. [Quick reference](#22-quick-reference)

---

## 1. Design philosophy

PipePlan keeps **control flow** and **data flow** strictly separate, exactly as
the architecture manifesto requires.

**Control flow is the orchestrator's job.** It reads each task's `depends_on`
array, *adds* the producer→consumer edges implied by `output`/`input`/`${pipe}`
operands (so ordering is always correct even when `depends_on` is omitted),
topologically sorts the result with `graphlib.TopologicalSorter`, rejects cycles
up front, and dispatches independent tasks concurrently.

**Data flow is the engine's job.** Tasks read named frames from, and write
results back to, a thread-safe `Dict[str, pd.DataFrame]` execution state via
explicit `input` and `output` declarations. Nothing is implicit: a frame must be
named to be read.

Three more principles run through the whole codebase:

- **Vectorised only.** There is no row-by-row Python and no `DataFrame.apply` in
  the hot path. Every transform pushes work through pandas' vectorised string,
  numeric, and indexing paths. Fuzzy matching follows the mandated pattern —
  extract unique keys, score them, broadcast winners back — so cost scales with
  key cardinality, not row count.
- **Strict typing.** Every blueprint construct is a Pydantic v2 model with
  `extra="forbid"`. A transform's parameters *are* its model fields, so parameter
  validation and business logic live in one place and a typo fails at parse time,
  not mid-run.
- **Pristine core.** Transforms register themselves through a decorator
  (`@register_transform`). The registry refuses to bind a name twice, so the set
  of verbs is open for extension but closed against accidental shadowing.

### The transform tiers

Transforms are classified by what axis of the data they touch:

| Tier | Cardinality | Verbs |
|------|-------------|-------|
| **Element** | 1:1 column-axis ops | `label`, `map`, `replace`, `cast`, `affix`, `normalize`, `derive`, `fillna`, `select`, `drop` |
| **Set** | row-axis masking / reshaping | `filter`, `sort`, `dedupe`, `group`, `window` |
| **Collection** | multi-frame relational | `merge`, `join`, `union`, `fuzzy_join`, `compare_diff` |

> **Note on `select`/`drop`.** Column projection alters the *column* axis and
> never masks a row, so these two verbs live in the **Element** tier alongside
> `cast`/`label`, not in Set. (They were briefly classified under Set during the
> v1 build and were subsequently reclassified.) The tier is a semantic
> `ClassVar` and does not gate execution — transforms apply identically
> regardless of tier — but the taxonomy is kept truthful.

---

## 2. Installation

```bash
pip install -e ".[dev]"          # core + openpyxl, pyarrow, rapidfuzz, pytest
# optional backends:
pip install -e ".[postgres]"     # psycopg2
pip install -e ".[access]"       # pyodbc + sqlalchemy-access (+ OS-level ODBC driver)
pip install -e ".[mysql]"        # pymysql
pip install -e ".[files]"        # openpyxl + pyarrow (Excel / parquet)
pip install -e ".[fuzzy]"        # rapidfuzz (else falls back to stdlib difflib)
```

Core runtime dependencies: `pandas>=2.0`, `pydantic>=2.5`, `sqlalchemy>=2.0`,
`python-dateutil`, and `PyYAML`. Python `>=3.11` is required (the DAG uses
`graphlib`, and the AST uses Pydantic v2.5+ callable discriminators).

---

## 3. Quick start

Run the bundled demo, which generates synthetic sources (a messy Excel workbook,
a regions JSON, a SQLite inventory DB), runs the modular blueprint under
`examples/demo/pipeline/`, and prints the resulting warehouse tables:

```bash
python examples/demo/run_demo.py
```

It is **idempotent** — re-running reuses transform checkpoints and produces
identical output (the incremental watermark advances, the SCD2 dimension keeps a
stable row count with no duplicate versions, the partitioned fact table is
stable).

Drive your own blueprint from the CLI:

```bash
pipeplan validate examples/demo/pipeline/pipeline.yaml --param run_date=2026-01-01
pipeplan run      examples/demo/pipeline/pipeline.yaml --param run_date=2026-01-01
pipeplan schema   > pipeplan.schema.json     # JSON Schema of the blueprint format
```

Or from Python:

```python
from pipeplan import load_config, run_pipeline

config = load_config("pipeline.yaml", params={"run_date": "2026-01-01"})
run_pipeline(config)
```

---

## 4. Blueprint anatomy

A blueprint is a single YAML document (fragments may be split across files — see
§5). The top-level keys are:

```yaml
apiVersion: pipeplan/v1          # required — pins the schema version
kind: Pipeline                   # required

metadata:                        # identity & ownership
  id: billing_pipeline
  owner: data-engineering
  description: Reference pipeline exercising contracts, incremental, and SCD2.

imports:                         # modular fragments, deep-merged (see §5)
  - vars.yaml
  - resources.yaml
  - schemas.yaml
  - tasks/extract.yaml
  - tasks/transform.yaml
  - tasks/load.yaml

settings:                        # global run settings
  timezone: America/New_York

parameters:                      # typed run parameters (see §7)
  run_date: { type: date, required: true }

defaults:                        # folded into every task (see §5)
  retry: { attempts: 3, delay: 30s, backoff: 2 }

orchestration:                   # scheduling, checkpoints, watermarks (see §16)
  max_parallelism: 4
  checkpoint: { enabled: true, store: "${env:CKPT}", reuse: true }
  watermark_store: { resource: warehouse, table: pipeplan_watermarks }

vars:                            # reusable values, referenced via ${var:...}
  status_decode: { C: completed, P: pending, X: cancelled }
  active_regions: [AM, EU]

schemas:                         # per-dataframe contracts (see §13)
  orders_final:
    primary_key: [order_id]
    columns:
      order_id: { dtype: integer, nullable: false, unique: true }
      amount:   { dtype: float, nullable: false }

resources:                       # connections to the outside world (see §8)
  warehouse:
    adapter: db
    params: { engine: postgresql, uri: "${secret:warehouse_uri}" }
    allow: [read, write]

tasks:                           # the DAG (see §9)
  extract_orders: { ... }
  clean_orders:   { ... }
  load_warehouse: { ... }
```

Only `apiVersion`, `kind`, `metadata`, `resources`, and `tasks` are conceptually
required for a pipeline that does real work; everything else is optional and can
arrive via imports.

---

## 5. Modular configuration

PipePlan is built around **extreme modularity**: a blueprint can be assembled
from many small fragment files. The loader executes a fixed pipeline:

1. **Read** the root document (YAML or JSON, detected by extension/content).
2. **Imports** — recursively load and **deep-merge** each fragment listed in
   `imports`. `resources`, `schemas`, `vars`, and `tasks` may be spread across
   files; mappings merge key-by-key, so each fragment contributes its slice.
3. **Templates & defaults** — apply `extends` task templates, then fold the
   global `defaults` block into every task.
4. **Parameters** — coerce and validate the supplied run parameters against the
   typed `parameters` schema.
5. **Interpolate** — resolve `${env|var|param|secret:...}` tokens over the parsed
   structure (`${pipe}` is preserved for runtime).
6. **Validate** — construct the Pydantic `PipelineConfig` model.

### Task templates (`extends`)

Define a template once, then have tasks inherit and override it. Deep-merge means
a task only states what differs from its template.

### Defaults

The global `defaults` block is folded into every task, so cross-cutting settings
(retry policy, `on_error` behaviour) are declared once. A task can still override
any defaulted key locally.

> **Tip.** Because the merge is deep and key-wise, splitting a large pipeline into
> `resources.yaml`, `schemas.yaml`, and `tasks/*.yaml` keeps each file focused and
> reviewable while still validating as one coherent pipeline.

---

## 6. Interpolation namespaces

Tokens are resolved **at load time** over the already-parsed structure, and
resolution is **type-aware**: a token that is the *whole* value yields the raw
value (so `"${var:active_regions}"` becomes a list, not a stringified list),
while a token embedded in a larger string is substituted textually.

| Token | Resolves from |
|-------|---------------|
| `${env:NAME}`    | the process environment |
| `${var:name}`    | the `vars:` block |
| `${param:name}`  | a typed run parameter |
| `${secret:path}` | the configured secret provider (value redacted in logs) |
| `${pipe}`        | **runtime only** — the frame currently flowing through a task (see §10) |

Resolution order matters: the `vars` block is resolved first (it may itself
reference `env`/`param`/`secret`), after which a `var` resolver is exposed to the
rest of the document. In **strict** mode (`--strict`) an unresolved token aborts
the load; otherwise unresolved tokens are left intact so that resources which are
never exercised cannot block a run. Secret values are collected into a sink and
redacted wherever the engine logs.

---

## 7. Run parameters

The `parameters` block declares typed, optionally-required run inputs. They are
coerced and validated before anything else, and bound from the CLI with
`--param KEY=VALUE` (repeatable) or from Python via `load_config(..., params=...)`.

```yaml
parameters:
  run_date:    { type: date, required: true }
  region:      { type: string, default: GLOBAL }
  full_reload: { type: bool, default: false }
```

Reference them anywhere in the blueprint with `${param:run_date}`. Because
coercion is typed, `--param run_date=2026-01-01` arrives as a real `date`, not a
string.

---

## 8. Resources and adapters

A **resource** is a named handle to something outside the pipeline. There are two
adapter families, and the distinction is a hard architectural rule, not a
convenience:

- **`file` adapters** use standard pandas I/O: Excel, CSV, TSV, JSON, parquet.
- **`db` adapters** use SQLAlchemy for **every** database — including local
  file-backed engines like **SQLite** and **MS Access** (via an ODBC connection
  string), *never* raw file reads. An Access `.accdb` or a `.sqlite` file is a
  `db` resource with a connection URI, not a `file` resource.

```yaml
resources:
  source_xlsx:
    adapter: file
    params: { format: excel, path: "${env:EXCEL_DIR}/billing.xlsx" }
    allow: [read]

  warehouse:
    adapter: db
    params: { engine: postgresql, uri: "${secret:warehouse_uri}" }
    allow: [read, write]

  legacy_access:
    adapter: db
    params:
      engine: access
      uri: "access+pyodbc://@${env:ACCESS_DIR}/legacy.accdb"
    allow: [read]
```

### The `allow` permission list

Each resource declares `allow: [read]`, `allow: [write]`, or both. The list is
enforced **inside the adapter**, so a task cannot bypass it — an extract step
against a write-only resource, or a load step against a read-only resource, fails
fast at execution. Declaring the permission a resource actually needs is part of
authoring a correct blueprint.

---

## 9. Tasks and steps

A **task** is a node in the DAG. Every task belongs to a `stage`
(`extract` | `transform` | `load`), optionally names `depends_on`, and contains a
list of ordered `steps`. Tasks read/write the shared state via `input` / `output`.

### Extract tasks

```yaml
extract_orders:
  stage: extract
  resource: source_xlsx          # scalar in v1 (was a list in pre-1.0)
  steps:
    - { collection: sheet_1, output: orders_raw }
    - { collection: sheet_2, output: customers_raw }
```

`collection` names the sheet/table/file-section to read; `output` names the
dataframe written into state.

### Transform tasks

```yaml
clean_orders:
  stage: transform
  input: orders_raw
  output: orders_clean
  output_contract: orders_clean  # optional schema contract (see §13)
  steps:
    - { action: label, with: { status_code: status } }
    - { action: cast,  with: { order_id: integer, amount: float } }
```

Each step is `{ action, with: {...} }`. The `with` block validates *directly* into
the named transform's Pydantic model. Steps run in sequence; the frame produced by
one step flows into the next (see §10).

### Load tasks

```yaml
load_warehouse:
  stage: load
  resource: warehouse
  depends_on: [finalize_orders]
  steps:
    - { collection: fact_orders, input: orders_final, mode: upsert, key: order_id }
```

`input` is the dataframe to write; `collection` is the destination table;
`mode` selects the write strategy (see §14).

### Per-step error handling

A step may declare `on_error` (and tasks inherit `defaults.on_error`). For
example, `cast` supports `on_error: quarantine`, which diverts unparseable rows
into a `<output>__rejected` frame instead of aborting the run.

---

## 10. The `${pipe}` rule

Within a task's `steps`, operations execute **sequentially**, and the dataframe
produced by one step is the input to the next. For most steps this is implicit.
For **collection** operations (`merge`, `join`, `union`, `compare_diff`) the
operands must be named — and you bind the currently-flowing frame with the
explicit `${pipe}` token.

```yaml
enrich_orders:
  stage: transform
  output: orders_enriched
  depends_on: [filter_orders, normalize_customers]
  steps:
    # first collection op: both operands named explicitly
    - action: merge
      with: { left: orders_filtered, right: customers_clean, how: left, on: cust_id }
    # second op: bind the result of the previous step with ${pipe}
    - action: join
      with: { left: "${pipe}", right: regions_raw, how: left, on: region_code }
```

**A missing operand is an error, never an implicit guess.** In the pre-1.0
format, omitting `left` implicitly fell back to the flowing frame; v1 retired that
behaviour to make data flow auditable. If you mean "the frame flowing through this
task", say `${pipe}`.

---

## 11. Transform catalogue

Every transform's `with` block is a strict Pydantic model — unknown keys are
rejected. The catalogue below groups verbs by tier.

### Element tier

1:1 vectorised column operations. Each input row maps to exactly one output row.

#### `label` — rename columns
```yaml
- action: label
  with: { Grant_ID: grant_id, Project_Title: title, Award_No: award_number }
```

#### `map` — value remapping per column
```yaml
- action: map
  with:
    status: { Open: Active, Closed: Inactive }
    department: { "computer science": comp_sci, mathematics: math }
```

#### `replace` — regex substitution per column
```yaml
- action: replace
  with:
    grant_id: { regex: "^jjc-", swap: "", flags: i }
    amount:   { regex: "[$,]",  swap: "" }
```

#### `cast` — coerce dtypes (with optional quarantine)
Supported targets include `integer`, `float`, `string`, `boolean`, `date`,
`datetime`. By default bad values fail loudly; `on_error: quarantine` diverts
unparseable rows to a `<output>__rejected` frame instead of nulling them silently.
```yaml
- action: cast
  on_error: quarantine
  with: { order_id: integer, amount: float, qty: integer, order_date: date }
```

#### `affix` — prefix/suffix string columns
```yaml
- action: affix
  with:
    grant_id: { text: "-award", position: suffix }
    grant_pln: { text: "JJC-",  position: prefix }
```

#### `normalize` — chained string normalisation
A column maps to an ordered list of operations (`nfkc`, `strip`, `lower`, `upper`,
`title`, …) applied left to right.
```yaml
- action: normalize
  with:
    title:  [nfkc, strip, upper]
    status: [strip, lower]
```

#### `derive` — compute a new column from an expression AST
```yaml
- action: derive
  with:
    target: line_total
    expr: { "*": [{ col: amount }, { col: qty }] }
```
See §12 for the full expression grammar.

#### `fillna` — fill missing values per column
```yaml
- action: fillna
  with: { discount: 0, region_code: UNKNOWN }
```

#### `select` — keep only the named columns (project + reorder)
Returns a copy in the requested order. **Raises by default** if any requested
column is absent — you cannot emit a column that does not exist. The bare-list
shorthand is supported.
```yaml
- action: select
  with: [grant_id, title, status, award_number]   # bare-list shorthand
# equivalently:
- action: select
  with: { columns: [grant_id, title, status], ignore_missing: true }
```

#### `drop` — remove the named columns (preserve survivor order)
**Lenient by default** — dropping an already-absent column is a no-op, so the
operation is idempotent. Set `ignore_missing: false` to make an absent target an
error.
```yaml
- action: drop
  with:
    columns: [_staging_hash, _ingest_batch]
    ignore_missing: false      # make an absent target an error
```

> The asymmetric `ignore_missing` defaults are deliberate: `select` is a hard
> requirement (the desired output literally cannot be produced if a column is
> missing), while `drop` is declarative — the goal state "column absent" is
> already satisfied when the column is missing. Both `select` and `drop` are pure
> label-based indexing, fully vectorised, and never mutate the input frame.

### Set tier

Row-axis masking and reshaping.

#### `filter` — keep rows matching a predicate AST
```yaml
- action: filter
  with:
    AND:
      - { status:   { op: "==", value: Active } }
      - { grant_id: { op: ">=", value: 100 } }
```
See §12 for the predicate grammar.

#### `sort`
```yaml
- action: sort
  with: { grant_id: asc, amount: desc }
```

#### `dedupe` — drop duplicate rows by key
```yaml
- action: dedupe
  with: { on: grant_pln, keep: first }
```

#### `group` — aggregate (requires an explicit `agg`)
`group` *always* aggregates; for key-wise de-duplication use `dedupe`. The `agg`
block is mandatory so intent is unambiguous.
```yaml
- action: group
  with:
    by: grant_id
    agg: { amount: sum, qty: sum }
```

#### `window` — partitioned window functions
```yaml
- action: window
  with:
    partition_by: [cust_id]
    order_by: { order_date: desc }
    add:
      recency_rank:   { fn: row_number }
      running_total:  { fn: cumsum, column: line_total }
```
Supported `fn` values include `row_number`, `cumcount`, `rank`, `dense_rank`, and
cumulative numeric ops (`cumsum`, `cummax`, …). Ranking/cumulative ops require a
`column`; `row_number`/`cumcount` do not.

### Collection tier

Relational operations across multiple frames. Operands are named; the flowing
frame is bound with `${pipe}` (see §10).

#### `merge` / `join`
```yaml
- action: merge
  with: { left: orders_filtered, right: customers_clean, how: left, on: cust_id }
- action: join
  with: { left: "${pipe}", right: regions_raw, how: left, on: region_code }
```

#### `union` — vertical concatenation
```yaml
- action: union
  with: { frames: ["${pipe}", late_arrivals], how: outer }
```

#### `fuzzy_join` — approximate key matching
Follows the mandated pattern: unique keys are extracted, scored
(`rapidfuzz` when available, stdlib `difflib` otherwise), and the winning matches
are broadcast back before an exact merge, so cost scales with key cardinality.

#### `compare_diff` — Change Data Capture (delta processing)

Compares two snapshots of the same logical dataset and classifies every business
key as `insert`, `update`, `delete`, or `unchanged`, emitting one tagged frame.

| Operation | Meaning | Values carried |
|-----------|---------|----------------|
| `insert` | key in `source` but not `target` | source |
| `update` | key in both, a compared column differs | source |
| `delete` | key in `target` but not `source` | target |
| `unchanged` | key in both, nothing compared differs | source |

Parameters:

| Field | Default | Meaning |
|-------|---------|---------|
| `source` | — | new/incoming snapshot (state name or `${pipe}`) |
| `target` | — | previous/baseline snapshot (state name or `${pipe}`) |
| `key` | — | business key column(s); single name or list (composite) |
| `compare` | all common non-key columns | columns whose values decide `update` vs `unchanged` |
| `ignore` | `[]` | columns excluded from comparison (applied after `compare`) |
| `change_detection` | `exact` | `exact` (null-safe, dtype-tolerant element-wise) or `hash` (per-row hash; faster on wide frames, assumes aligned dtypes) |
| `emit` | `[insert, update, delete]` | which change types appear in the output; add `unchanged` for a full audit |
| `op_column` | `_op` | name of the emitted operation column |
| `op_labels` | `{}` | per-type literal overrides, e.g. `{insert: I, update: U, delete: D}` |
| `duplicate_keys` | `error` | `error`, or `keep_first` / `keep_last` to dedupe each snapshot first |

Behavioural guarantees:

- Fully vectorised — identity is an `Index.isin` membership test; change detection
  runs an inner merge over the **common keys only**. No row loops, no `.apply`.
- `exact` is dtype-tolerant (`1` vs `1.0` is *not* a change) and treats
  `NaN → NaN` as unchanged but `value ↔ NaN` as a change.
- The operation column is emitted **first**; output is in canonical order
  (insert, update, delete, unchanged) regardless of `emit` ordering. A collision
  between `op_column` and an existing column is a hard error.

```yaml
capture_changes:
  stage: transform
  output: billing_delta
  depends_on: [extract_snapshot, extract_baseline]
  steps:
    - action: compare_diff
      with:
        source: billing_snapshot
        target: warehouse_baseline
        key: grant_id
        compare: [title, status, award_number]
        emit: [insert, update, delete]
        op_column: _op
        op_labels: { insert: I, update: U, delete: D }
```

The resulting `billing_delta` then feeds a load step in `mode: upsert` (for the
I/U rows) or `mode: delete` (for the D rows), giving incremental loads instead of
full-table replaces. `compare_diff` pairs naturally with the SCD2 load mode (§14)
for full historical dimensions.

---

## 12. The AST subsystems

Two subsystems — filtering and compute — are recursive JSON/YAML **abstract
syntax trees**. Both are validated by Pydantic v2 callable discriminators: the
node type is inferred from the keys present, so the blueprint carries **no
redundant type tags**. Every node evaluates to a vectorised pandas result.

### Predicate AST (used by `filter`)

Internal nodes are logical combinators; leaves are column comparisons.

```yaml
AND:
  - { status: { op: "==", value: Active } }
  - OR:
      - { percentile:  { op: ">=", value: 90 } }
      - { finish_time: { op: "<=", value: "10.00" } }
  - NOT: { region_code: { op: in, value: [AM, EU] } }
```

A comparison is `{ column: { op, value } }`. Supported operators:

| Category | Operators |
|----------|-----------|
| Equality / ordering | `==`, `!=`, `>`, `>=`, `<`, `<=` |
| Membership | `in`, `not_in` |
| Range | `between` (value is `[low, high]`) |
| Null | `isnull`, `notnull` |
| String | `contains`, `startswith`, `endswith` |

Scalar values are coerced to the column's dtype before comparison (so
`value: "10.00"` against a float column compares numerically). Referencing an
unknown column raises a clear error listing the available columns.

### Expression AST (used by `derive`)

Expressions are trees that evaluate to a column (`pd.Series`) or scalar. Leaves
are `{ col: name }` (column reference) or `{ lit: value }` (constant). Arithmetic
nodes key on the operator symbol with a list of operands; function nodes use
`{ fn, args }`.

```yaml
# (line_total) - (unit_cost * qty)
"-":
  - { col: line_total }
  - { "*": [{ col: unit_cost }, { col: qty }] }

# round(ratio, 2)
{ fn: round, args: [{ col: ratio }, { lit: 2 }] }
```

Built-in functions dispatch through the **expression registry**, which is
extensible via the `pipeplan.expressions` entry-point group (§19).

---

## 13. Schema contracts and expectations

The `schemas` block declares per-dataframe **contracts** — fail-fast structural
guarantees that turn deep-in-the-run surprises into declarative validation.

```yaml
schemas:
  orders_final:
    strict: false                 # if true, no columns beyond those declared
    primary_key: [order_id]
    foreign_keys:
      - { column: region_code, references: regions.region_code }
    columns:
      order_id: { dtype: integer, nullable: false, unique: true }
      amount:   { dtype: float, nullable: false, allowed: null }
      status:   { dtype: string, allowed: [completed, pending, cancelled] }
```

Per-column knobs: `dtype` (matched by family — `integer`/`int`/`bigint`,
`float`/`double`, `number`/`numeric`, `string`/`str`/`text`, `boolean`/`bool`,
`date`/`datetime`/`timestamp`, `category`), `nullable`, `unique`, `allowed`
(value whitelist), and `checks` (additional comparison assertions).

**Attaching a contract.** Tasks reference contracts via `input_contract`,
`output_contract`, or a per-step `contract`. Enforcement is configured with
`validate: off | warn | strict`.

**Expectations** are softer data-quality assertions (row counts, null rates,
value bounds) that can `warn` rather than `fail`:

```yaml
expectations:
  - { name: keys_present, assert: { column: order_id, not_null: true } }
  - { name: nonempty,     assert: { row_count: { ">=": 1 } } }
  - name: amounts_sane
    assert: { column: amount, min: 0 }
    on_failure: warn            # warn | fail
```

Both contracts and expectations are Pydantic models (validated at parse time) but
run their checks vectorised against the live frame.

---

## 14. Loading: write modes

Every load step names a `mode`. All modes are **idempotent** for a given
key/partition, and loaders support chunked writes.

| Mode | Behaviour |
|------|-----------|
| `replace` | overwrite the target; optionally scoped to a partition via `write.partition_by` |
| `append` | add rows, no key logic |
| `upsert` | insert new keys, update existing ones (requires `key`) |
| `delete` | remove rows matching the supplied keys |
| `scd2` | Type-2 slowly-changing dimension: maintains history via `effective_from` / `effective_to` / `current_flag` |

```yaml
- { collection: fact_orders, input: orders_final, mode: upsert, key: order_id }

- collection: dim_customer
  input: customer_delta
  mode: scd2
  key: cust_id
  scd2: { effective_from: valid_from, effective_to: valid_to, current_flag: is_current }
```

Partition-scoped replace only rewrites the partitions present in the incoming
frame, which keeps large fact tables stable across re-runs:

```yaml
- collection: fact_orders
  input: orders_final
  mode: replace
  write: { partition_by: [order_date] }
```

---

## 15. Incremental extract and watermarks

An extract step can read only the rows whose cursor advanced since the last
successful run, using a persisted high-water mark.

```yaml
- collection: orders
  output: orders_raw
  incremental:
    strategy: watermark
    cursor: updated_at        # the monotonic column to track
    lookback: 1d              # safety window re-read on each run
    initial: "2026-01-01"     # high-water mark for the very first run
```

The watermark is read from and written to the store configured under
`orchestration.watermark_store`. For `db` resources the predicate is **pushed
down** into the query, so only changed rows are fetched. The `lookback` window
guards against late-arriving rows by re-reading a small trailing slice each run.

---

## 16. Orchestration

The `orchestration` block tunes how the DAG is scheduled and made resumable.

```yaml
orchestration:
  max_parallelism: 4
  checkpoint:
    enabled: true
    store: "${env:CKPT}"      # parquet-backed checkpoint directory
    reuse: true               # resume by reusing prior step outputs
  watermark_store: { resource: warehouse, table: pipeplan_watermarks }
  sla: 30m                    # raise/notify if the run exceeds this
```

- **Parallel scheduling.** Independent tasks run concurrently via a
  `ThreadPoolExecutor`, up to `max_parallelism`. Ordering comes from the
  topological sort over `depends_on` plus the data-derived producer/consumer
  edges.
- **Checkpoints.** With `checkpoint.enabled`, each task's output frame is
  persisted as parquet. On a re-run with `reuse: true`, completed steps are
  restored from the checkpoint store instead of recomputed — this is what makes
  the demo idempotent and resumable after a partial failure.
- **Failure handling.** A task may declare `on_failure` behaviour; per-step
  `on_error` (e.g. `cast`'s `quarantine`) handles row-level problems without
  aborting.
- **SLA tracking.** If the run exceeds the configured `sla` duration, the engine
  flags it (and can notify — §17). Durations accept human units: `30s`, `10m`,
  `2h`, `7d`.

---

## 17. Secrets and notifiers

**Secret providers** resolve `${secret:...}` tokens. The default provider is
environment-backed; custom providers (Vault, AWS Secrets Manager, …) plug in via
the `pipeplan.secret_providers` entry-point group. Resolved secret values are
tracked and **redacted** wherever the engine logs.

**Notifiers** report run outcomes (success, failure, SLA breach) through a
pluggable registry, contributed via the `pipeplan.notifiers` entry-point group.
The core ships the registry and wiring; concrete channels (Slack, email, …) are
extensions.

---

## 18. Command-line interface

```bash
pipeplan validate <blueprint> [--param K=V ...] [--strict]
pipeplan run      <blueprint> [--param K=V ...] [--strict]
pipeplan schema
```

- **`validate`** loads and statically validates the blueprint, prints the
  resolved execution order, and exits — it touches no data.
- **`run`** validates, then executes the pipeline.
- **`schema`** prints the JSON Schema of the blueprint format (useful for editor
  autocompletion/validation).

Flags: `--param KEY=VALUE` (repeatable) binds run parameters; `--strict` makes any
unresolved `${...}` token a hard error; `-v` / `-vv` raise log verbosity.

From Python the public API is `load_config`, `run_pipeline`, `Orchestrator` (for
finer control), `PipelineConfig`, and `ExecutionContext`. Importing the package
registers all built-in transforms and expressions as a side effect, so a freshly
loaded config can run immediately.

---

## 19. Extending PipePlan

The core never imports third-party transforms directly. Contribute new verbs,
expression functions, notifiers, and secret providers by declaring entry points in
your own package's `pyproject.toml`:

```toml
[project.entry-points."pipeplan.transforms"]
my_action = "my_pkg.module:MyTransform"

[project.entry-points."pipeplan.expressions"]
geodistance = "my_pkg.module:geodistance"

[project.entry-points."pipeplan.notifiers"]
slack = "my_pkg.module:make_slack_notifier"

[project.entry-points."pipeplan.secret_providers"]
vault = "my_pkg.module:make_vault_provider"
```

### Writing a transform

A transform is a Pydantic model that declares its tier and implements `apply`:

```python
from typing import ClassVar
import pandas as pd
from pipeplan.core.context import ExecutionContext
from pipeplan.core.registry import register_transform
from pipeplan.transforms.base import Tier, Transform

@register_transform("my_action")
class MyTransform(Transform):
    tier: ClassVar[Tier] = Tier.ELEMENT
    column: str
    factor: float = 1.0

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        out = df.copy()
        out[self.column] = out[self.column] * self.factor   # vectorised
        return out
```

The model fields *are* the validated `with` parameters (`extra="forbid"` rejects
typos). Keep `apply` fully vectorised — no `iterrows`/`itertuples`/`.apply`. If
you build a transform in its own module, remember its decorator only runs when the
module is imported; register it by adding `from . import my_module  # noqa: F401`
to `transforms/__init__.py`. The registry refuses to bind a name twice, so to
*replace* a built-in you must first remove the original registration.

---

## 20. Migrating from the pre-1.0 JSON format

Version 1.0 (`apiVersion: pipeplan/v1`, authored in YAML or JSON) supersedes the
pre-1.0 single-file JSON blueprint. The two describe the same pipeline; v1 renames
a few keys for clarity, makes some implicit behaviour explicit, and adds modular
imports, typed parameters, contracts, expectations, incremental extract, more load
modes, and orchestration.

### Key and structural mapping

| Pre-1.0 (JSON) | v1 (YAML) |
|----------------|-----------|
| `version: "..."` | `apiVersion: pipeplan/v1` |
| `pipeline_id: x` | `metadata: { id: x }` |
| `timezone: ...` | `settings: { timezone: ... }` |
| task `dependency: [...]` | task `depends_on: [...]` |
| extract step `output_dataframe` | extract step `output` |
| transform `input_dataframe` / `output_dataframe` | `input` / `output` |
| transform step `{ action, params: {...} }` | `{ action, with: {...} }` |
| load step `dataframe` | load step `input` |
| task `resource: [one]` (list) | task `resource: one` (scalar) |
| `${VAR}` | `${env:VAR}` (also `${var:}`, `${param:}`, `${secret:}`) |
| `derive` expr `{ op, left, right }` | operator-keyed `{ "*": [lhs, rhs] }` |

### Behavioural changes (implicit → explicit)

- **Piped data is explicit.** A collection op must name its operands; bind the
  flowing frame with `${pipe}`. Omitting the operand is now an error.
- **`group` requires `agg`.** Use `dedupe` for key-wise de-duplication; `group`
  always aggregates.
- **Outputs must be unique.** Two tasks/steps producing the same dataframe name is
  a config error.
- **`depends_on` is additive.** The orchestrator always adds data-derived edges,
  so `depends_on` only needs the *extra* ordering constraints you want — it can
  never under-specify the run order.

### Four bugs fixed in the reference template

The annotated `examples/master_blueprint.yaml` corrects four defects carried by
the original JSON template:

1. The Excel resource is read in an extract step, so it must `allow: [read]` (the
   original allowed only `write`).
2. Directory tokens use the `${env:...}` namespace (the original had a typo'd
   `${ACESS_DIR}`).
3. The Postgres URI has no JDBC prefix — SQLAlchemy URLs are not JDBC URLs.
4. Every `db` resource names its backend with a single, consistent `engine` key
   (the original mixed `engine` and `format`).

---

## 21. End-to-end worked example

A compact but complete pipeline: extract a messy Excel snapshot, clean it,
capture the delta against the warehouse baseline, and upsert the changes.

```yaml
apiVersion: pipeplan/v1
kind: Pipeline

metadata: { id: billing_delta, owner: data-engineering }

settings: { timezone: America/New_York }

parameters:
  run_date: { type: date, required: true }

vars:
  status_decode: { Open: Active, Closed: Inactive }

resources:
  source_xlsx:
    adapter: file
    params: { format: excel, path: "${env:EXCEL_DIR}/billing.xlsx" }
    allow: [read]
  warehouse:
    adapter: db
    params: { engine: postgresql, uri: "${secret:warehouse_uri}" }
    allow: [read, write]

schemas:
  billing_clean:
    primary_key: [grant_id]
    columns:
      grant_id: { dtype: integer, nullable: false, unique: true }
      status:   { dtype: string, allowed: [Active, Inactive] }

orchestration:
  max_parallelism: 2
  checkpoint: { enabled: true, store: "${env:CKPT}", reuse: true }
  watermark_store: { resource: warehouse, table: pipeplan_watermarks }

tasks:

  extract_snapshot:
    stage: extract
    resource: source_xlsx
    steps:
      - { collection: grants, output: billing_raw }

  extract_baseline:
    stage: extract
    resource: warehouse
    steps:
      - { collection: dim_grant, output: warehouse_baseline }

  clean_billing:
    stage: transform
    input: billing_raw
    output: billing_clean
    output_contract: billing_clean
    steps:
      - { action: label, with: { Grant_ID: grant_id, Project_Title: title, Award_No: award_number } }
      - { action: replace, with: { grant_id: { regex: "^jjc-", swap: "", flags: i } } }
      - { action: cast, on_error: quarantine, with: { grant_id: integer } }
      - { action: map, with: { status: "${var:status_decode}" } }
      - { action: normalize, with: { title: [nfkc, strip, upper] } }
      - { action: select, with: [grant_id, title, status, award_number] }
    expectations:
      - { name: keys_present, assert: { column: grant_id, not_null: true } }

  capture_changes:
    stage: transform
    output: billing_delta
    depends_on: [clean_billing, extract_baseline]
    steps:
      - action: compare_diff
        with:
          source: billing_clean
          target: warehouse_baseline
          key: grant_id
          compare: [title, status, award_number]
          emit: [insert, update, delete]
          op_labels: { insert: I, update: U, delete: D }

  load_changes:
    stage: load
    resource: warehouse
    depends_on: [capture_changes]
    steps:
      - { collection: dim_grant, input: billing_delta, mode: upsert, key: grant_id }
```

Validate, then run:

```bash
pipeplan validate pipeline.yaml --param run_date=2026-01-01 --strict
pipeplan run      pipeline.yaml --param run_date=2026-01-01
```

---

## 22. Quick reference

**Top-level keys:** `apiVersion`, `kind`, `metadata`, `imports`, `settings`,
`parameters`, `defaults`, `orchestration`, `vars`, `schemas`, `resources`,
`tasks`.

**Interpolation:** `${env:}`, `${var:}`, `${param:}`, `${secret:}`, `${pipe}`
(runtime-only).

**Tiers & verbs:**
- Element — `label`, `map`, `replace`, `cast`, `affix`, `normalize`, `derive`,
  `fillna`, `select`, `drop`
- Set — `filter`, `sort`, `dedupe`, `group` (needs `agg`), `window`
- Collection — `merge`, `join`, `union`, `fuzzy_join`, `compare_diff`

**Filter operators:** `==` `!=` `>` `>=` `<` `<=` `in` `not_in` `between`
`isnull` `notnull` `contains` `startswith` `endswith`; combinators `AND`/`OR`/`NOT`.

**Expression nodes:** `{ col: }`, `{ lit: }`, operator-keyed `{ "*": [...] }`,
`{ fn:, args: }`.

**Load modes:** `replace` (partition-scoped), `append`, `upsert`, `delete`,
`scd2`.

**Incremental:** `incremental: { strategy: watermark, cursor, lookback, initial }`.

**CLI:** `pipeplan validate|run|schema`, flags `--param K=V`, `--strict`, `-v/-vv`.

**Entry-point groups:** `pipeplan.transforms`, `pipeplan.expressions`,
`pipeplan.notifiers`, `pipeplan.secret_providers`.

**Exceptions:** `PipePlanError` (base), `ConfigError`, `InterpolationError`,
`DependencyError`, `StateError`, `RegistryError`, `AdapterError`,
`PermissionDeniedError`, `TransformError`, `ExpressionError`, `ContractError`,
`ExpectationError`.
