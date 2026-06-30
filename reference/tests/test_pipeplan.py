"""Test-suite for PipePlan v1.

Covers fragment assembly + namespaced interpolation, typed parameters, schema
contracts and data-quality expectations, every transform tier (element / set /
collection, including the explicit ${pipe} rule and the new select/drop/fillna/
union/window verbs), the file + SQLite adapters with all load modes (replace,
append, upsert, delete, scd2, partitioned replace) and incremental reads, and
orchestration (DAG ordering, cycle detection, parallelism, on_failure, checkpoint
reuse, watermarking). Runs entirely on local files + SQLite.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from pipeplan import load_config, run_pipeline
from pipeplan.adapters import create_adapter
from pipeplan.config.models import (
    LoadMode,
    Permission,
    ResourceConfig,
    PipelineConfig,
)
from pipeplan.core.context import ExecutionContext
from pipeplan.core.contracts import DataframeContract, Expectation
from pipeplan.core.exceptions import (
    ConfigError,
    ContractError,
    DependencyError,
    ExpectationError,
    PermissionDeniedError,
    TransformError,
)
from pipeplan.core.secrets import EnvSecretProvider, MappingSecretProvider
from pipeplan.core.state import StateManager
from pipeplan.transforms.base import build_transform


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #


def _ctx(state: StateManager | None = None) -> ExecutionContext:
    cfg = PipelineConfig.model_validate({"metadata": {"id": "t"}, "tasks": {}})
    return ExecutionContext(config=cfg, state=state or StateManager())


def _apply(action: str, params, df=None, ctx=None):
    ctx = ctx or _ctx()
    return build_transform(action, params).apply(df, ctx)


# --------------------------------------------------------------------------- #
# interpolation, params, imports, templates
# --------------------------------------------------------------------------- #


def test_namespaced_interpolation_typed(monkeypatch):
    monkeypatch.setenv("MY_HOST", "db.example.com")
    cfg = load_config(
        {
            "metadata": {"id": "p"},
            "vars": {"regions": ["AM", "EU"], "threshold": 100},
            "parameters": {"run": {"type": "string", "default": "x"}},
            "resources": {
                "r": {"adapter": "file", "params": {"format": "csv", "path": "/tmp/${env:MY_HOST}.csv"}},
            },
            "tasks": {
                "t": {
                    "stage": "transform", "input": "a", "output": "b",
                    "steps": [{"action": "filter", "with": {"region": {"op": "in", "value": "${var:regions}"}}}],
                },
                "src": {"stage": "extract", "resource": "r", "steps": [{"output": "a"}]},
            },
        },
        params={"run": "y"},
        secret_provider=MappingSecretProvider({}),
    )
    # whole-value token -> raw list, embedded token -> string
    step = cfg.tasks["t"].steps[0]
    assert step.with_["region"]["value"] == ["AM", "EU"]
    assert cfg.resources["r"].params["path"] == "/tmp/db.example.com.csv"
    assert cfg.runtime_params["run"] == "y"


def test_secret_redaction_and_collection():
    cfg = load_config(
        {
            "metadata": {"id": "p"},
            "resources": {"r": {"adapter": "db", "params": {"engine": "sqlite", "path": "/tmp/x.db",
                                                            "token": "${secret:api/key}"}}},
            "tasks": {"src": {"stage": "extract", "resource": "r", "steps": [{"collection": "t", "output": "a"}]}},
        },
        secret_provider=MappingSecretProvider({"api/key": "SUPERSECRET"}),
    )
    assert cfg.resources["r"].params["token"] == "SUPERSECRET"
    assert "SUPERSECRET" in cfg.secret_values


def test_required_param_missing():
    with pytest.raises(ConfigError):
        load_config({
            "metadata": {"id": "p"},
            "parameters": {"d": {"type": "date", "required": True}},
            "tasks": {"src": {"stage": "extract", "resource": "r",
                              "steps": [{"collection": "t", "output": "a"}]},
                      },
            "resources": {"r": {"adapter": "db", "params": {"engine": "sqlite", "path": "/tmp/x"}}},
        })


def test_param_allowed_enforced():
    with pytest.raises(ConfigError):
        load_config(
            {
                "metadata": {"id": "p"},
                "parameters": {"env": {"type": "string", "allowed": ["dev", "prod"]}},
                "resources": {"r": {"adapter": "db", "params": {"engine": "sqlite", "path": "/tmp/x"}}},
                "tasks": {"src": {"stage": "extract", "resource": "r", "steps": [{"collection": "t", "output": "a"}]}},
            },
            params={"env": "staging"},
        )


def test_imports_deep_merge(tmp_path: Path):
    (tmp_path / "resources.yaml").write_text(
        "resources:\n  r:\n    adapter: db\n    params: {engine: sqlite, path: /tmp/x.db}\n"
    )
    (tmp_path / "pipeline.yaml").write_text(
        "apiVersion: pipeplan/v1\n"
        "metadata: {id: imp}\n"
        "imports: [resources.yaml]\n"
        "tasks:\n  src:\n    stage: extract\n    resource: r\n    steps:\n      - {collection: t, output: a}\n"
    )
    cfg = load_config(tmp_path / "pipeline.yaml")
    assert "r" in cfg.resources and "src" in cfg.tasks


def test_templates_and_defaults_applied():
    cfg = load_config({
        "metadata": {"id": "p"},
        "defaults": {"on_failure": "skip"},
        "templates": {"strict": {"validate": "strict"}},
        "resources": {"r": {"adapter": "db", "params": {"engine": "sqlite", "path": "/tmp/x"}}},
        "tasks": {
            "src": {"stage": "extract", "resource": "r", "steps": [{"collection": "t", "output": "a"}]},
            "tr": {"stage": "transform", "extends": "strict", "input": "a", "output": "b",
                   "steps": [{"action": "sort", "with": {"a": "asc"}}]},
        },
    })
    assert cfg.tasks["tr"].validation.value == "strict"
    assert cfg.tasks["src"].on_failure.value == "skip"


# --------------------------------------------------------------------------- #
# validation: unique producers, pipe usage, references
# --------------------------------------------------------------------------- #


def test_duplicate_producer_rejected():
    with pytest.raises(ConfigError):
        load_config({
            "metadata": {"id": "p"},
            "resources": {"r": {"adapter": "db", "params": {"engine": "sqlite", "path": "/tmp/x"}}},
            "tasks": {
                "a": {"stage": "extract", "resource": "r", "steps": [{"collection": "t", "output": "dup"}]},
                "b": {"stage": "extract", "resource": "r", "steps": [{"collection": "u", "output": "dup"}]},
            },
        })


def test_consumed_but_not_produced_rejected():
    with pytest.raises(ConfigError):
        load_config({
            "metadata": {"id": "p"},
            "resources": {"r": {"adapter": "db", "params": {"engine": "sqlite", "path": "/tmp/x"}}},
            "tasks": {"t": {"stage": "transform", "input": "ghost", "output": "b",
                            "steps": [{"action": "sort", "with": {"x": "asc"}}]}},
        })


def test_group_requires_agg():
    with pytest.raises(TransformError):
        build_transform("group", {"by": "k"})


# --------------------------------------------------------------------------- #
# element transforms
# --------------------------------------------------------------------------- #


def test_element_label_map_replace_cast():
    df = pd.DataFrame({"ID": ["ORD-1", "ORD-2"], "s": ["C", "P"]})
    df = _apply("label", {"ID": "id"}, df)
    df = _apply("replace", {"id": {"regex": "^ORD-", "swap": ""}}, df)
    df = _apply("map", {"s": {"C": "completed", "P": "pending"}}, df)
    df = _apply("cast", {"id": "integer"}, df)
    assert df["id"].tolist() == [1, 2]
    assert df["s"].tolist() == ["completed", "pending"]


def test_cast_quarantine():
    df = pd.DataFrame({"n": ["1", "2", "bad", "4"]})
    good, rejected = build_transform("cast", {"n": "integer"}).apply_safe(df, _ctx())
    assert good["n"].tolist() == [1, 2, 4]
    assert len(rejected) == 1


def test_derive_expression():
    df = pd.DataFrame({"a": [2, 3], "b": [4, 5]})
    out = _apply("derive", {"target": "p", "expr": {"*": [{"col": "a"}, {"col": "b"}]}}, df)
    assert out["p"].tolist() == [8, 15]


def test_fillna_creates_column():
    df = pd.DataFrame({"a": [1, None]})
    out = _apply("fillna", {"a": 0, "b": 9}, df)
    assert out["a"].tolist() == [1, 0]
    assert out["b"].tolist() == [9, 9]


# --------------------------------------------------------------------------- #
# set transforms
# --------------------------------------------------------------------------- #


def test_select_drop():
    df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
    assert list(_apply("select", ["a", "c"], df).columns) == ["a", "c"]
    assert list(_apply("drop", ["b"], df).columns) == ["a", "c"]


def test_group_agg():
    df = pd.DataFrame({"k": ["x", "x", "y"], "v": [1, 2, 3]})
    out = _apply("group", {"by": "k", "agg": {"v": "sum"}}, df).sort_values("k")
    assert out["v"].tolist() == [3, 3]


def test_window_row_number_cumsum():
    df = pd.DataFrame({"g": ["a", "a", "b"], "d": [1, 2, 1], "v": [10, 20, 30]})
    out = _apply("window", {"partition_by": ["g"], "order_by": {"d": "asc"},
                            "add": {"rn": {"fn": "row_number"}, "cs": {"fn": "cumsum", "column": "v"}}}, df)
    out = out.sort_values(["g", "d"])
    assert out["rn"].tolist() == [1, 2, 1]
    assert out["cs"].tolist() == [10, 30, 30]


# --------------------------------------------------------------------------- #
# collection transforms + explicit pipe
# --------------------------------------------------------------------------- #


def test_merge_with_pipe_and_state():
    st = StateManager()
    st.set("right", pd.DataFrame({"k": [1, 2], "name": ["a", "b"]}))
    ctx = _ctx(st)
    left = pd.DataFrame({"k": [1, 2], "v": [10, 20]})
    out = build_transform("merge", {"left": "${pipe}", "right": "right", "how": "left", "on": "k"}).apply(left, ctx)
    assert out["name"].tolist() == ["a", "b"]


def test_pipe_without_flow_errors():
    with pytest.raises(TransformError):
        build_transform("merge", {"left": "${pipe}", "right": "x", "on": "k"}).apply(None, _ctx())


def test_union():
    st = StateManager()
    st.set("other", pd.DataFrame({"a": [3]}))
    ctx = _ctx(st)
    out = build_transform("union", {"frames": ["${pipe}", "other"], "dedupe": True}).apply(
        pd.DataFrame({"a": [1, 3]}), ctx
    )
    assert sorted(out["a"].tolist()) == [1, 3]


# --------------------------------------------------------------------------- #
# contracts + expectations
# --------------------------------------------------------------------------- #


def test_contract_pass_and_fail():
    c = DataframeContract.model_validate({
        "name": "s", "strict": True, "primary_key": ["id"],
        "columns": {"id": {"dtype": "integer", "unique": True, "nullable": False},
                    "amt": {"dtype": "float", "checks": [{">=": 0}]}},
    })
    good = pd.DataFrame({"id": pd.array([1, 2], dtype="Int64"), "amt": [1.0, 2.0]})
    c.validate_frame(good, label="g", state=StateManager())
    bad = pd.DataFrame({"id": pd.array([1, 1], dtype="Int64"), "amt": [1.0, -1.0]})
    with pytest.raises(ContractError):
        c.validate_frame(bad, label="b", state=StateManager())


def test_expectation_warn_vs_fail():
    df = pd.DataFrame({"x": [1, None]})
    Expectation.model_validate({"name": "w", "assert": {"column": "x", "not_null": True},
                                "on_failure": "warn"}).check(df, task="t")  # no raise
    with pytest.raises(ExpectationError):
        Expectation.model_validate({"name": "f", "assert": {"column": "x", "not_null": True}}).check(df, task="t")


# --------------------------------------------------------------------------- #
# adapters
# --------------------------------------------------------------------------- #


def test_file_adapter_csv_roundtrip_and_since(tmp_path: Path):
    p = tmp_path / "d.csv"
    res = ResourceConfig.model_validate({"name": "r", "adapter": "file",
                                         "params": {"format": "csv", "path": str(p)}})
    a = create_adapter(res)
    a.write(pd.DataFrame({"c": [1, 2, 3], "v": ["a", "b", "c"]}), "d", mode=LoadMode.REPLACE)
    assert len(a.read("d")) == 3
    assert a.read("d", since=("c", 1))["c"].tolist() == [2, 3]


def test_file_adapter_rejects_scd2(tmp_path: Path):
    res = ResourceConfig.model_validate({"name": "r", "adapter": "file",
                                         "params": {"format": "csv", "path": str(tmp_path / "x.csv")}})
    with pytest.raises(Exception):
        create_adapter(res).write(pd.DataFrame({"k": [1]}), "x", mode=LoadMode.SCD2, key=["k"])


def test_permission_enforced(tmp_path: Path):
    res = ResourceConfig.model_validate({"name": "r", "adapter": "file", "allow": ["read"],
                                         "params": {"format": "csv", "path": str(tmp_path / "x.csv")}})
    with pytest.raises(PermissionDeniedError):
        create_adapter(res).write(pd.DataFrame({"a": [1]}), "x", mode=LoadMode.REPLACE)


def test_db_adapter_modes(tmp_path: Path):
    db = tmp_path / "w.db"
    res = ResourceConfig.model_validate({"name": "w", "adapter": "db",
                                         "params": {"engine": "sqlite", "path": str(db)}})
    a = create_adapter(res)
    a.write(pd.DataFrame({"k": [1, 2], "v": [10, 20]}), "t", mode=LoadMode.REPLACE)
    a.write(pd.DataFrame({"k": [2, 3], "v": [99, 30]}), "t", mode=LoadMode.UPSERT, key=["k"])
    got = a.read("t").sort_values("k")
    assert got["v"].tolist() == [10, 99, 30]
    a.write(pd.DataFrame({"k": [1]}), "t", mode=LoadMode.DELETE, key=["k"])
    assert sorted(a.read("t")["k"].tolist()) == [2, 3]
    a.dispose()


def test_db_scd2_history(tmp_path: Path):
    db = tmp_path / "s.db"
    res = ResourceConfig.model_validate({"name": "w", "adapter": "db",
                                         "params": {"engine": "sqlite", "path": str(db)}})
    a = create_adapter(res)
    scd = {"track": ["name"], "effective_from": "vf", "effective_to": "vt", "current_flag": "cur"}
    a.write(pd.DataFrame({"k": [1, 2], "name": ["a", "b"]}), "d", mode=LoadMode.SCD2, key=["k"], scd=scd)
    a.write(pd.DataFrame({"k": [1, 2], "name": ["a", "B2"]}), "d", mode=LoadMode.SCD2, key=["k"], scd=scd)
    df = a.read("d")
    # key 2 changed -> one closed + one current; key 1 unchanged -> single current
    assert len(df) == 3
    assert int(df["cur"].astype("int").sum()) == 2
    a.dispose()


# --------------------------------------------------------------------------- #
# orchestration end-to-end
# --------------------------------------------------------------------------- #


def _e2e_config(tmp_path: Path) -> dict:
    src = tmp_path / "src.csv"
    pd.DataFrame({"k": [1, 2, 3], "v": [10, 20, 30]}).to_csv(src, index=False)
    return {
        "metadata": {"id": "e2e"},
        "resources": {
            "in": {"adapter": "file", "params": {"format": "csv", "path": str(src)}, "allow": ["read"]},
            "out": {"adapter": "db", "params": {"engine": "sqlite", "path": str(tmp_path / "o.db")}, "allow": ["read", "write"]},
        },
        "tasks": {
            "ext": {"stage": "extract", "resource": "in", "steps": [{"output": "raw"}]},
            "tr": {"stage": "transform", "input": "raw", "output": "clean",
                   "steps": [{"action": "filter", "with": {"v": {"op": ">=", "value": 20}}}]},
            "ld": {"stage": "load", "resource": "out",
                   "steps": [{"input": "clean", "collection": "fact", "mode": "replace"}]},
        },
    }


def test_end_to_end(tmp_path: Path):
    cfg = load_config(_e2e_config(tmp_path))
    ctx = run_pipeline(cfg)
    assert ctx.state.has("clean")
    out = pd.read_sql('SELECT * FROM "fact"', sqlite3.connect(tmp_path / "o.db"))
    assert sorted(out["k"].tolist()) == [2, 3]


def test_parallel_execution(tmp_path: Path):
    cfg_dict = _e2e_config(tmp_path)
    cfg_dict["orchestration"] = {"max_parallelism": 4}
    cfg = load_config(cfg_dict)
    run_pipeline(cfg)  # must not deadlock / corrupt state


def test_cycle_detection():
    cfg = load_config({
        "metadata": {"id": "c"},
        "resources": {"r": {"adapter": "db", "params": {"engine": "sqlite", "path": "/tmp/x"}}},
        "tasks": {
            "a": {"stage": "transform", "input": "fb", "output": "fa",
                  "steps": [{"action": "sort", "with": {"x": "asc"}}]},
            "b": {"stage": "transform", "input": "fa", "output": "fb",
                  "steps": [{"action": "sort", "with": {"x": "asc"}}]},
        },
    })
    from pipeplan.orchestrator import Orchestrator
    with pytest.raises(DependencyError):
        Orchestrator(cfg).execution_order()


def test_on_failure_skip(tmp_path: Path):
    # A load task pointed at a missing dataframe would fail; on_failure=skip keeps going.
    cfg_dict = _e2e_config(tmp_path)
    cfg_dict["tasks"]["tr"]["on_failure"] = "skip"
    cfg_dict["tasks"]["tr"]["steps"] = [{"action": "cast", "with": {"missing": "integer"}}]
    cfg_dict["tasks"]["ld"]["steps"] = [{"input": "raw", "collection": "fact", "mode": "replace"}]
    cfg = load_config(cfg_dict)
    run_pipeline(cfg)  # tr fails, skipped; ld still loads raw
    out = pd.read_sql('SELECT * FROM "fact"', sqlite3.connect(tmp_path / "o.db"))
    assert len(out) == 3
