# repository.py

from dagster_code.schedules import etl_schedule
from dagster import Definitions
from dagster_code.jobs import etl_job

defs = Definitions(
    jobs=[etl_job],
    schedules=[etl_schedule]
)
