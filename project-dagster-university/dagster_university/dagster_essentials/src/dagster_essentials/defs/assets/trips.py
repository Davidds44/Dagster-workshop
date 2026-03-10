import requests
from dagster_essentials.defs.assets import constants
from dagster_essentials.defs.partitions import monthly_partition, weekly_partition
import dagster as dg
import os
import duckdb
from dagster_duckdb import DuckDBResource

# month_to_fetch = '2023-03'

@dg.asset(
    partitions_def=monthly_partition
)
def taxi_trips_file(context: dg.AssetExecutionContext) -> None:
    """
        The raw parquet fles for the taxi trips datasets. Sorced from open source NYC data
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3] # extract the month from the partition key, which is in the format YYYY-MM

    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)
    
@dg.asset
def taxi_zones_file() -> None:
    """
        The raw csv file for the taxi zones dataset. Sorced from open source NYC data
    """
    raw_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_zones.content)



@dg.asset(
    partitions_def=monthly_partition,
    deps=[taxi_trips_file]
)
def taxi_trips(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    """
        The parquet files for the taxi trips datasets read into duckdb tables. 
    """

    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3] # extract the month from the partition key, which is in the format YYYY-MM

    query = f"""
        CREATE TABLE if not exists trips (
            VendorID INTEGER,
            pickup_zone_id INTEGER,
            dropoff_zone_id INTEGER,
            rate_code_id INTEGER,
            payment_type VARCHAR,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            trip_distance DOUBLE,
            passenger_count INTEGER,
            total_amount DOUBLE,
            partition_date varchar
        );

        Delete from trips where partition_date = '{month_to_fetch}';

        insert into trips
        select
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_pickup_datetime as pickup_datetime,
            tpep_dropoff_datetime as dropoff_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount,
            '{month_to_fetch}' as partition_date
        from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
        
    """

    with database.get_connection() as conn:
        conn.execute(query)



@dg.asset(
    deps=[taxi_zones_file]
)
def taxi_zones(database: DuckDBResource) -> None:
    """
        The csv file for the taxi zones dataset read into a duckdb table. 
    """
    query = f"""
        CREATE OR REPLACE TABLE zones AS (
        select
            LocationID as zone_id,
            Borough as borough,
            Zone as zone,
            the_geom as geometry
        from '{constants.TAXI_ZONES_FILE_PATH}'
        )
    """

    with database.get_connection() as conn:
        conn.execute(query)