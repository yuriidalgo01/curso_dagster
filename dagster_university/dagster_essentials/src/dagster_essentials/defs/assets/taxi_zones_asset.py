# src/dagster_essentials/defs/assets/trips.py
import os
import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster._utils.backoff import backoff
from dagster_essentials.defs.assets import constants

# src/dagster_essentials/defs/assets/trips.py
@dg.asset(
    deps=["taxi_zones_file"]
)
def taxi_zones(database: DuckDBResource) -> None:
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    query = f"""
        create or replace table zones as (
          select
            LocationID  as zone_id,
            zone,
            borough,
            the_geom as geometry,
          from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """
    # O DuckDBResource gerencia a conexão com segurança
    with database.get_connection() as conn:
        conn.execute(query)
