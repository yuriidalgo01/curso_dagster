# src/dagster_essentials/defs/assets/trips.py
import requests
import dagster as dg
from dagster_essentials.defs.assets import constants
from dagster_essentials.defs.partitions import monthly_partition

@dg.asset(
        partitions_def = monthly_partition
)
def taxi_trips_file(context: dg.AssetExecutionContext) -> None:
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """

    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)
