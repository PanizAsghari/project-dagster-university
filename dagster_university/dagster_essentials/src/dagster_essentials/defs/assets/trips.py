import dagster as dg
import requests
from dagster_essentials.defs.assets import constants
from dagster_essentials.defs.partitions import montly_partition
from dagster_duckdb import DuckDBResource


@dg.asset(
        partitions_def=montly_partition
)
def taxi_trip_file(context: dg.AssetExecutionContext) -> None:
    partition_date_str = context.partition_key
    month_to_fetch= partition_date_str[:-3]
    raw_trips= requests.get(
         f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )
    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output:
        output.write(raw_trips.content)

@dg.asset
def taxi_zones_file() -> None:
    raw_taxi_zones = requests.get("https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv")
    with open(constants.TAXI_ZONES_FILE_PATH,"wb") as output:
        output.write(raw_taxi_zones.content) 

@dg.asset(
    deps= ["taxi_trip_file"]
)
def taxi_trips(context: dg.AssetExecutionContext,database: DuckDBResource) -> None:
    partition_date_str = context.partition_key
    month_to_fetch= partition_date_str[:-3]
    create_query=""" create table if not exists as (
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
            total_amount as total_amount,
            '{month_to_fetch}' as partition_date
          from 'data/raw/taxi_trips_{month_to_fetch}.parquet'
          where 1=0
        );"""
    delete_query="""delete from trips where partition_date='{month_to_fetch}'"""
    insert_query = f"""
        insert into trips
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
            total_amount as total_amount,
            '{month_to_fetch}' as partition_date
          from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}'
        ;
    """

    with database.get_connection() as conn:
        conn.execute(create_query)
        conn.execute(delete_query)
        conn.execute(insert_query)

@dg.asset(
    deps=["taxi_zones_file"]
)
def taxi_zones(database: DuckDBResource) -> None:
    query = f"""
        create or replace table zones as (
          select
            LocationID as zone_id,
            zone as zone,
            borough,
            the_geom as geometry,
          from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)
