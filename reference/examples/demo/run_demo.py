"""Run the PipePlan v1 demo end to end.

Generates synthetic source data, points DATA_DIR + the checkpoint dir at a
temp area, executes the modular blueprint under ``pipeline/``, and prints the
resulting warehouse tables. Re-running is idempotent (and the second run will
reuse transform checkpoints).
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
CKPT_DIR = DATA_DIR / "_checkpoints"
BLUEPRINT = HERE / "pipeline" / "pipeline.yaml"


def main() -> int:
    import generate_data

    generate_data.main()
    CKPT_DIR.mkdir(parents=True, exist_ok=True)
    os.environ["DATA_DIR"] = str(DATA_DIR)
    os.environ["PIPEPLAN_CHECKPOINT_DIR"] = str(CKPT_DIR)

    # Import after env is set so paths resolve.
    import pipeplan

    config = pipeplan.load_config(BLUEPRINT, params={"run_date": "2026-01-01"})
    ctx = pipeplan.run_pipeline(config)

    print("\n=== run complete ===")
    print("materialised:", ", ".join(ctx.state.names()))

    wh = DATA_DIR / "warehouse.db"
    conn = sqlite3.connect(wh)
    try:
        tables = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", conn
        )["name"].tolist()
        for table in tables:
            df = pd.read_sql(f'SELECT * FROM "{table}"', conn)
            print(f"\n--- {table} ({len(df)} rows) ---")
            with pd.option_context("display.width", 140, "display.max_columns", 20):
                print(df.to_string(index=False))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
