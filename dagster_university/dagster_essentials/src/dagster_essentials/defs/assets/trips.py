import dagster as dg
import requests
from dagster_essentials.defs.assets import constants

@dg.asset
def taxi_trip_file() -> None:
    month_to_fetch='2023-01'
    raw_trips= requests.get(
         f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )
    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output:
        output.write(raw_trips.content)
