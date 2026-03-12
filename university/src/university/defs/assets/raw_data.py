import dagster as dg
from dagster_duckdb import DuckDBResource


def _load_csv_into_table(
    *, table_name: str, url: str, database: DuckDBResource
) -> int:
    """
    Load a remote CSV into a DuckDB table and return the row count.
    """
    with database.get_connection() as conn:
        conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS "
            "SELECT * FROM read_csv_auto(?, HEADER=TRUE)",
            [url],
        )
        row_count = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}",
        ).fetchone()[0]
    return int(row_count)


@dg.asset(
    group_name="raw_data",
    description=(
        "Raw customers from the Jaffle Shop classic dataset loaded into the "
        "DuckDB table raw_customers."
    ),
)
def raw_customers(database: DuckDBResource) -> dg.MaterializeResult:
    row_count = _load_csv_into_table(
        table_name="raw_customers",
        url=(
            "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/"
            "refs/heads/main/seeds/raw_customers.csv"
        ),
        database=database,
    )

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "table": dg.MetadataValue.text("raw_customers"),
        }
    )


@dg.asset(
    group_name="raw_data",
    description=(
        "Raw orders from the Jaffle Shop classic dataset loaded into the "
        "DuckDB table raw_orders."
    ),
)
def raw_orders(database: DuckDBResource) -> dg.MaterializeResult:
    row_count = _load_csv_into_table(
        table_name="raw_orders",
        url=(
            "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/"
            "refs/heads/main/seeds/raw_orders.csv"
        ),
        database=database,
    )

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "table": dg.MetadataValue.text("raw_orders"),
        }
    )


@dg.asset(
    group_name="raw_data",
    description=(
        "Raw payments from the Jaffle Shop classic dataset loaded into the "
        "DuckDB table raw_payments."
    ),
)
def raw_payments(database: DuckDBResource) -> dg.MaterializeResult:
    row_count = _load_csv_into_table(
        table_name="raw_payments",
        url=(
            "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/"
            "refs/heads/main/seeds/raw_payments.csv"
        ),
        database=database,
    )

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "table": dg.MetadataValue.text("raw_payments"),
        }
    )

