"""Generate synthetic source data for the PipePlan demo.

Creates, under ``examples/demo/data/``:

* ``sales.xlsx``     -- two sheets: ``orders`` and ``customers`` (deliberately
  messy: prefixed IDs, currency-formatted amounts, coded statuses, untidy names);
* ``regions.json``   -- a records array mapping region codes to names;
* ``inventory.db``   -- a SQLite database with a ``products`` table.

Run it directly: ``python examples/demo/generate_data.py``.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent / "data"


def _orders() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": ["ORD-1001", "ORD-1002", "ORD-1003", "ORD-1004",
                         "ORD-1005", "ORD-1006", "ORD-1003"],  # 1003 duplicated
            "cust_id": [1, 2, 1, 3, 2, 4, 1],
            "product_id": [10, 11, 12, 10, 13, 11, 12],
            "status_code": ["C", "P", "C", "C", "X", "C", "C"],
            "amount": ["$1,200.50", "$89.00", "$450.00", "$1,999.99",
                       "$15.00", "$640.25", "$450.00"],
            "qty": [2, 1, 3, 1, 5, 2, 3],
            "order_date": ["2026-01-05", "2026-01-06", "2026-01-07",
                           "2026-01-08", "2026-01-09", "2026-01-10", "2026-01-07"],
            # A junk column the `select` step prunes away.
            "notes": ["", "rush", "", "gift", "", "", ""],
        }
    )


def _customers() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "cust_id": [1, 2, 3, 4],
            "cust_name": ["  alice ANDERSON ", "bob brown", "  CAROL  CHEN",
                          "dan davis  "],
            "region_code": ["AM", "EU", "AP", "EU"],
        }
    )


def _regions() -> list[dict[str, str]]:
    return [
        {"region_code": "AM", "region_name": "North America"},
        {"region_code": "EU", "region_name": "Europe"},
        {"region_code": "AP", "region_name": "Asia Pacific"},
    ]


def _products() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "product_id": [10, 11, 12, 13],
            "product_name": ["Widget", "Gadget", "Gizmo", "Doohickey"],
            "unit_cost": [120.0, 30.0, 75.0, 2.5],
            # Cursor for the incremental (watermark) extract.
            "updated_at": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"],
        }
    )


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    xlsx_path = DATA_DIR / "sales.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        _orders().to_excel(writer, sheet_name="orders", index=False)
        _customers().to_excel(writer, sheet_name="customers", index=False)

    (DATA_DIR / "regions.json").write_text(
        json.dumps(_regions(), indent=2), encoding="utf-8"
    )

    inv_path = DATA_DIR / "inventory.db"
    if inv_path.exists():
        inv_path.unlink()
    conn = sqlite3.connect(inv_path)
    try:
        _products().to_sql("products", conn, index=False, if_exists="replace")
    finally:
        conn.close()

    # A fresh warehouse each run keeps the demo deterministic.
    wh_path = DATA_DIR / "warehouse.db"
    if wh_path.exists():
        wh_path.unlink()

    print(f"wrote {xlsx_path}")
    print(f"wrote {DATA_DIR / 'regions.json'}")
    print(f"wrote {inv_path}")


if __name__ == "__main__":
    main()
