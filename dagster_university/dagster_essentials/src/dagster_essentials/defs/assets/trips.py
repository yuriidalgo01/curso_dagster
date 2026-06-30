# src/dagster_essentials/defs/assets/trips.py
import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_essentials.defs.assets import constants
#from dagster_essentials.defs.partitions import monthly_partition

@dg.asset(
    deps=["taxi_trips_file"],
)
def taxi_trips(database: DuckDBResource) -> None:
    query = """
        create or replace table taxi_trips as (
          select
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount
          from 'data/raw/taxi_trips_2023-03.parquet'
        );
    """
    
    # O DuckDBResource gerencia a conexão com segurança
    with database.get_connection() as conn:
        conn.execute(query)