# Migrating to PipePlan v1

Version 1.0 is the canonical format (`apiVersion: pipeplan/v1`). The pre-1.0
single-file JSON blueprint is superseded. The two formats are conceptually the
same pipeline; v1 renames a few keys for clarity, makes some implicit behaviour
explicit, and adds modular `imports`, typed parameters, schema contracts,
expectations, incremental extract, more load modes, and orchestration.

## Key and structural mapping

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

## Behavioural changes (make implicit explicit)

- **Piped data is explicit.** A collection op (`merge`/`join`/`union`) must name
  its operands; bind the flowing frame with the `${pipe}` token. Omitting the
  operand is now an error rather than an implicit fallback.
- **`group` requires `agg`.** For key-wise de-duplication use `dedupe`; `group`
  always aggregates, so intent is unambiguous.
- **Outputs must be unique.** Two tasks/steps producing the same dataframe name
  is a config error.
- **`depends_on` is additive.** The orchestrator always adds data-derived edges,
  so `depends_on` only needs the extra ordering constraints you want; it can
  never under-specify the run order.

## Minimal example

Before (pre-1.0 JSON):

```json
{
  "version": "0.1",
  "pipeline_id": "sales",
  "resources": { "src": { "adapter": "file", "params": { "format": "csv", "path": "${DATA}/s.csv" } } },
  "tasks": {
    "ext": { "stage": "extract", "resource": ["src"], "steps": [{ "output_dataframe": "raw" }] },
    "tr": { "stage": "transform", "input_dataframe": "raw", "output_dataframe": "clean",
            "steps": [{ "action": "group", "params": { "by": "k" } }] }
  }
}
```

After (v1 YAML):

```yaml
apiVersion: pipeplan/v1
metadata: { id: sales }
resources:
  src: { adapter: file, params: { format: csv, path: "${env:DATA}/s.csv" } }
tasks:
  ext: { stage: extract, resource: src, steps: [{ output: raw }] }
  tr:
    stage: transform
    input: raw
    output: clean
    steps:
      - { action: group, with: { by: k, agg: { v: sum } } }   # agg now required
```

## New capabilities to adopt

- **`imports`** to split a blueprint into `resources.yaml`, `schemas.yaml`,
  `tasks/*.yaml`, deep-merged at load (the importing document wins).
- **`parameters`** (typed, with `--param k=v`) and a `vars:` block.
- **`schemas:`** contracts plus task `input_contract`/`output_contract` and
  `expectations`.
- **`incremental`** watermark extract; **load modes** `upsert`/`delete`/`scd2`
  and `write.partition_by`; **orchestration** parallelism, checkpoint/resume,
  watermark store, notifications, and SLA.

See `examples/master_blueprint.yaml` for a complete annotated reference and
`examples/demo/pipeline/` for a runnable modular layout.
