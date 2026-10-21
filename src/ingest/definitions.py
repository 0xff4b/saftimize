"""Dagster entry point: explicit date-range runs and daily UTC scheduling."""
from datetime import date, datetime, timedelta, timezone

from dagster import (
    Backoff, DefaultScheduleStatus, Definitions, Failure, Field, OpExecutionContext, RetryPolicy,
    RunRequest, ScheduleEvaluationContext, job, op, schedule,
)

from .cli import main as ingest_main

WINDOW_CONFIG = {
    "start": Field(str, default_value=""),
    "end": Field(str, default_value=""),
    "lookback_days": Field(int, default_value=3),
}
RETRY = RetryPolicy(max_retries=2, delay=10, backoff=Backoff.EXPONENTIAL)


def window(config: dict) -> tuple[str, str]:
    end = date.fromisoformat(config["end"]) if config.get("end") else datetime.now(timezone.utc).date()
    lookback = config.get("lookback_days", 3)
    if lookback < 1:
        raise ValueError("lookback_days must be positive")
    start = date.fromisoformat(config["start"]) if config.get("start") else end - timedelta(days=lookback)
    if start >= end:
        raise ValueError("start must precede end")
    return start.isoformat(), end.isoformat()


@op(config_schema=WINDOW_CONFIG, retry_policy=RETRY)
def ingest_electricity(context: OpExecutionContext) -> dict[str, str]:
    start, end = window(context.op_config)
    if ingest_main(["entsoe", "--start", start, "--end", end]) != 0:
        raise Failure("ENTSO-E ingestion failed; see sanitized step output.")
    return {"start": start, "end": end}


@op(retry_policy=RETRY)
def transform_hourly(context: OpExecutionContext, bounds: dict[str, str]) -> int:
    from .transform import main as transform_main

    if transform_main(["--start", bounds["start"], "--end", bounds["end"]]) != 0:
        raise Failure("Hourly transformation failed; see step output.")
    context.log.info("Hourly transformation completed for %s to %s", bounds["start"], bounds["end"])
    return 0


@job
def daily_energy():
    transform_hourly(ingest_electricity())


@schedule(job=daily_energy, cron_schedule="0 7 * * *", execution_timezone="UTC", default_status=DefaultScheduleStatus.RUNNING)
def daily_energy_schedule(context: ScheduleEvaluationContext):
    scheduled = context.scheduled_execution_time or datetime.now(timezone.utc)
    end = scheduled.astimezone(timezone.utc).date()
    start = end - timedelta(days=3)
    yield RunRequest(
        run_key=f"daily-energy-{end.isoformat()}",
        run_config={"ops": {"ingest_electricity": {"config": {"start": start.isoformat(), "end": end.isoformat()}}}},
    )


defs = Definitions(jobs=[daily_energy], schedules=[daily_energy_schedule])
