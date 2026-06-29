import dagster as dg

from dagster_and_dbt.defs.jobs import trip_update_job, weekly_update_job

trip_update_schedule = dg.ScheduleDefinition(
    job=trip_update_job,
    cron_schedule="45/10 9-11 * * 1-5",  # every 5th of the month at midnight
)

weekly_update_schedule = dg.ScheduleDefinition(
    job=weekly_update_job,
    cron_schedule="45/10 9-11 * * 1-5",  # every Monday at midnight
)
