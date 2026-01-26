from typing import Union

import dagster as dg
from dagster_essentials.defs.jobs import trip_update_job,weekly_update_job

trip_update_schedule = dg.ScheduleDefinition(
    name="trip_update_schedule",
    cron_schedule="0 0 5 * *",
    job=trip_update_job
)
weekly_trip_update_schedule = dg.ScheduleDefinition(
  cron_schedule="0 0 * * 1",
  job=weekly_update_job
)

@dg.schedule(cron_schedule="@daily", target="*")
def schedules(context: dg.ScheduleEvaluationContext) -> Union[dg.RunRequest, dg.SkipReason]:
    return dg.SkipReason("Skipping. Change this to return a RunRequest to launch a run.")
