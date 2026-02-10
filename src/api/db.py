"""DuckDB connection helper — read-only access to the processed database."""

import duckdb
from pathlib import Path
from functools import lru_cache

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH      = PROJECT_ROOT / "data" / "processed" / "housing.duckdb"


@lru_cache(maxsize=1)
def get_connection() -> duckdb.DuckDBPyConnection:
    """Return a cached read-only DuckDB connection."""
    return duckdb.connect(str(DB_PATH), read_only=True)


def query(sql: str, params: list | None = None):
    con = get_connection()
    if params:
        return con.execute(sql, params).fetchall()
    return con.execute(sql).fetchall()


def query_df(sql: str, params: list | None = None):
    con = get_connection()
    if params:
        return con.execute(sql, params).df()
    return con.execute(sql).df()
