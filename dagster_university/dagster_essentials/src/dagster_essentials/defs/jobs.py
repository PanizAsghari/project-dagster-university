import dagster as dg
from dagster_essentials.defs.partitions import montly_partition,weekly_partition


trips_by_week = dg.AssetSelection.assets("trips_by_week")


trip_update_job = dg.define_asset_job(
    name="trip_update_job",
    partitions_def=montly_partition, # partitions added here
    selection=dg.AssetSelection.assets("taxi_trip_file", "taxi_trips")
)
weekly_update_job = dg.define_asset_job(
    name="weekly_update_job",
    partitions_def=weekly_partition,
    selection=trips_by_week
)

