import dagster as dg
from dagster_essentials.defs.partitions import montly_partition,weekly_partition

trips_by_week= dg.AssetSelection.assets("trips_by_week")
trip_update_job=dg.define_asset_job(
    name="trip_update_job",
    selection=dg.AssetSelection.assets("taxi_trips","taxi_trip_file"),
    partitions_def=montly_partition
)
weekly_trip_update_job=dg.define_asset_job(
    name="weekly_trip_update_job",
    selection=trips_by_week,
    partitions_def=weekly_partition
)
adhoc_request = dg.AssetSelection.assets(["adhoc_request"])
adhoc_request_job = dg.define_asset_job(
    name="adhoc_request_job",
    selection=adhoc_request
)
@dg.job
def jobs():
    pass

