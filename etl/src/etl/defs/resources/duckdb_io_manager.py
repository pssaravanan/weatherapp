from pathlib import Path

import dagster as dg
from dagster_duckdb_pandas import DuckDBPandasIOManager

DUCKDB_PATH = Path(__file__).resolve().parents[4] / "storage" / "weather.duckdb"


@dg.definitions
def resources() -> dg.Definitions:
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

    return dg.Definitions(
        resources={
            "io_manager": DuckDBPandasIOManager(database=str(DUCKDB_PATH)),
        }
    )
