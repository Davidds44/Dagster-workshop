from __future__ import annotations

import dagster as dg

from .jobs import raw_data_daily_job


raw_data_daily_schedule = dg.ScheduleDefinition(
    job=raw_data_daily_job,
    cron_schedule="0 8 * * *",  # Every day at 08:00 (UTC by default)
)

