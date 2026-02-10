"""Bronze layer — raw EPC CSV ingestion into DuckDB, no transformations."""

import duckdb
import os
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_CSV      = PROJECT_ROOT / "data" / "raw" / "certificates.csv"
DB_PATH      = PROJECT_ROOT / "data" / "processed" / "housing.duckdb"


def run(db_path: str = str(DB_PATH), csv_path: str = str(RAW_CSV)) -> dict:
    print("[bronze] Starting ingestion...")

    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("DROP TABLE IF EXISTS bronze.epc_raw")

    con.execute(f"""
        CREATE TABLE bronze.epc_raw AS
        SELECT
            *,
            '{csv_path}'      AS _source_file,
            CURRENT_TIMESTAMP AS _ingested_at
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
    """)

    row_count = con.execute("SELECT COUNT(*) FROM bronze.epc_raw").fetchone()[0]
    col_count = con.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema='bronze' AND table_name='epc_raw'"
    ).fetchone()[0]

    con.close()

    print(f"[bronze] Done -- {row_count:,} rows, {col_count} columns -> bronze.epc_raw")
    return {
        "layer":     "bronze",
        "table":     "bronze.epc_raw",
        "rows":      row_count,
        "columns":   col_count,
        "db_path":   db_path,
        "completed": datetime.now().isoformat(),
    }


if __name__ == "__main__":
    run()
