# schedules.py (opsional kalau kamu mau schedule)

from dagster import ScheduleDefinition
from dagster_code.jobs import etl_job

etl_schedule = ScheduleDefinition(
    job=etl_job,
    cron_schedule="30 3,4,5 * * *",  # Jam 1, 7, dan 12 setiap hari
)

