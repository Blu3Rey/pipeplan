# PipePlan — Changelog

Reconciliation of the supporting documentation (starter guide, master blueprint,
tests) against the latest iteration of the framework. Grouped as the update
request specified: **Additions**, **Modifications**, **Deletions**. The prior
documentation baseline described the initial `pipeplan/v1` build (two hard-coded
adapter families, four entry-point groups, write modes as fixed branches).

---

## Additions

### Adapters are a registry-driven extension point
- New entry-point group **`pipeplan.adapters`**. Resource families are resolved
  from an `ADAPTERS` registry by a `create_adapter` factory; a new family is a
  registered `Adapter` subclass, addable out-of-tree with no core edit.
- Formal adapter contract: `read(collection) -> DataFrame` and
  `write_batch(requests)` over the shared `LoadMode` vocabulary, plus
  `WriteRequest` / `LoadResult` value types. `allow` enforcement lives in the base.

### New `document` adapter family (JSON / NoSQL)
- Targets the shape of MongoDB / DynamoDB / Firestore / CouchDB.
- **Nested documents flatten to dotted columns on read and are rebuilt on write**,
  so the tiered pandas transforms operate on a flat frame while the sink still
  receives real nested documents.
- Implements `replace` / `append` / `upsert` / `delete` natively per-document by
  key with no SQL; **rejects `scd2`** as a relational-only pattern.

### SQL dialect layer for the `db` family
- New entry-point group **`pipeplan.sql_dialects`**. `SqlLoadTarget` owns load
  mechanics (connection, staging, introspection); a `SqlDialect` (resolved via
  `resolve_dialect(engine)`) owns the *spelling* of each statement.
- Ships mainstream compilation (correlated-subquery `UPDATE`, native `ON CONFLICT`
  upsert, `1`/`0` booleans) and an **MS Access** dialect (JOIN-based
  `UPDATE`/`DELETE`/history-close, simulated update-then-insert upsert, `-1`/`0`
  booleans).

### Write modes are pluggable load strategies
- New entry-point group **`pipeplan.load_strategies`**. Each `mode` is a
  registered strategy rather than a hardcoded branch, so `merge`, soft-delete,
  `scd4`, etc. are addable out-of-tree.

### `case` node in the compute AST
- `derive` can now select a value by condition. Each `when` clause pairs a
  **predicate** (the full `filter` grammar) with an **expression** branch; `default`
  is the fallback. Compiles to a vectorised `np.select`; nests arbitrarily; needed
  no change to the `derive` transform.

### Formalized load guarantees
- **Atomic per task** — a load task's steps run in one transaction (SQLite
  included, via transactional DDL); retrying a failed load task is safe.
- **Watermark commits only on success** — an incremental cursor advances after the
  dependent load succeeds, never at read time.
- **Deterministic incremental SCD2** — only current rows' key + tracked columns
  are read; superseded versions closed with one set-based `UPDATE`; unchanged rows
  never rewritten.
- **Atomic file writes** — `file` adapter writes via temp-file + rename, with
  column-aligned appends.

### `compare_diff` Collection CDC transform
- Classifies business keys as `insert` / `update` / `delete` / `unchanged` between
  two snapshots (see the guide §11). *(Introduced in the delta-processing work;
  now documented as a first-class Collection verb.)*

### External plugin ecosystem (validation of the extension surface)
- Reference out-of-tree distributions exercise the entry-point mechanism end to
  end across tiers and I/O: Set-tier `pivot`/`unpivot`; Element-tier `bucketize`
  and Collection-tier `key_filter`; and a REST integration bundling a
  `transform` + a custom `adapter` in one atomically-versioned package.

---

## Modifications

### `adapter` kind: closed enum → open validated string
- The `AdapterKind` enum (`file` | `db`) is replaced by an open string validated
  against the `ADAPTERS` registry. New resource families are first-class without a
  schema change. **Blueprints are unaffected** — `adapter: file` / `adapter: db`
  still validate.

### `db` adapter relocated and layered
- The relational adapter now lives in its own **`adapters/sql/`** subpackage and
  delegates statement spelling to the dialect layer above, instead of inlining SQL
  per operation.

### `replace` is now schema-preserving
- `replace` **truncates and reloads** rather than dropping and recreating the
  target, so primary keys, indexes, and column types survive across runs. New
  targets get DDL from the step's **schema contract** (typed columns, `NOT NULL`,
  primary key) rather than pandas type inference.

### `select` / `drop` reclassified Set → Element
- Column projection alters the column axis and never masks a row, so both verbs are
  **Element-tier**. Shared `_Projection` base; asymmetric `ignore_missing`
  defaults (`select` raises by default, `drop` is lenient by default); copy
  semantics guarantee input-frame immutability; bare-list shorthand supported.

### Entry-point groups: 4 → 7
- Was: `pipeplan.transforms`, `pipeplan.expressions`, `pipeplan.notifiers`,
  `pipeplan.secret_providers`.
- Now additionally: `pipeplan.adapters`, `pipeplan.sql_dialects`,
  `pipeplan.load_strategies`.

### Load modes are strategies, not branches
- The five modes (`replace`/`append`/`upsert`/`delete`/`scd2`) are unchanged as
  *names*, but each is now resolved from the load-strategy registry (see Additions).

---

## Deletions

### `AdapterKind` enum removed
- The closed two-value adapter enum no longer exists; `adapter` is an open string.
  Any documentation or code referring to `AdapterKind` as a fixed set is stale.

### "Only two adapter families" model removed
- The framing that `file` and `db` are the *only* adapter families is retired:
  three families ship in-tree (`file`, `db`, `document`) and the set is open.

### Inlined per-operation load SQL removed
- Load-mode SQL is no longer written inline at each operation; it is produced by
  the dialect compiler. Docs describing hand-written per-mode SQL are stale.

### (Carried from the v1 cutover) pre-1.0 JSON format and implicit-pipe fallback
- The pre-1.0 single-file JSON blueprint and the implicit "flowing frame" fallback
  for collection operands remain retired; collection operands must be named or use
  `${pipe}`. Retained here for completeness — see guide §20.

---

## Documentation updated in this pass
- **PipePlan_User_Guide.md** — §8 rewritten (registry adapters, `document` family,
  SQL dialects); §12 gains the `case` node; §14 reframed as load strategies with
  the new guarantees; §19 expanded to all seven entry-point groups plus an
  adapter-authoring example and packaging guidance; §22 quick reference refreshed;
  currency banner added.
- **master_blueprint.yaml** — `document` resource + document load task; `case`
  derive example; header/resources/load comments updated for the registry-driven
  adapter, dialect, and load-strategy layers.
- **tests/test_pipeplan_reference.py** — refreshed suite covering the document
  round-trip, `case`, projection tiers, load-strategy dispatch, and adapter
  registry resolution (authored against the documented contracts; run it next to
  the live package and reconcile any signature drift).