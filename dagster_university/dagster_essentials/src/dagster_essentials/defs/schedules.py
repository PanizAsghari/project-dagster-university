import dagster as dg
from dagster_essentials.defs.jobs import trip_update_job, weekly_trip_update_job

trip_update_schedule = dg.ScheduleDefinition(
  name="trip_update_schedule",
  cron_schedule="0 0 5 * *",
  job=trip_update_job,
)

weekly_trip_update_schedule = dg.ScheduleDefinition(
  cron_schedule="0 0 * * 1",
  job=weekly_trip_update_job,
)
