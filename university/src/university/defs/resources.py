from __future__ import annotations

import os
from pathlib import Path

from dagster_duckdb import DuckDBResource


DB_ENV_VAR = "UNIVERSITY_DUCKDB_PATH"
DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "university.duckdb"


def _get_db_path() -> str:
    override = os.getenv(DB_ENV_VAR)
    if override:
        return override
    return str(DEFAULT_DB_PATH)


# Shared DuckDB resource for all assets.
# The database path can be overridden via the UNIVERSITY_DUCKDB_PATH
# environment variable.
database = DuckDBResource(database=_get_db_path())

