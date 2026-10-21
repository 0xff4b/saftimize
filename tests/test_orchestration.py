from datetime import datetime, timezone

from dagster import build_schedule_context
import pytest

from ingest import definitions


def test_explicit_window():
    assert definitions.window({"start": "2026-03-28", "end": "2026-03-30"}) == ("2026-03-28", "2026-03-30")


@pytest.mark.parametrize("config", [{"lookback_days": 0}, {"start": "2026-10-20", "end": "2026-10-19"}])
def test_invalid_window(config):
    with pytest.raises(ValueError):
        definitions.window(config)


def test_schedule_uses_scheduled_date_not_wall_clock():
    context = build_schedule_context(scheduled_execution_time=datetime(2026, 3, 30, 7, tzinfo=timezone.utc))
    request = list(definitions.daily_energy_schedule(context))[0]
    assert request.run_key == "daily-energy-2026-03-30"
    assert request.run_config["ops"]["ingest_electricity"]["config"] == {"start": "2026-03-27", "end": "2026-03-30"}


def test_job_dependency_and_execution(monkeypatch):
    calls = []
    monkeypatch.setattr(definitions, "ingest_main", lambda argv: calls.append(("ingest", argv)) or 0)
    from ingest import transform
    monkeypatch.setattr(transform, "main", lambda argv: calls.append(("transform", argv)) or 0)
    result = definitions.daily_energy.execute_in_process(run_config={"ops": {"ingest_electricity": {"config": {"start": "2026-10-17", "end": "2026-10-18"}}}})
    assert result.success
    assert [call[0] for call in calls] == ["ingest", "transform"]
    assert calls[0][1] == ["entsoe", "--start", "2026-10-17", "--end", "2026-10-18"]
    assert calls[1][1] == ["--start", "2026-10-17", "--end", "2026-10-18"]


def test_ingestion_failure_blocks_transform(monkeypatch):
    monkeypatch.setattr(definitions, "ingest_main", lambda argv: 1)
    result = definitions.daily_energy.execute_in_process(raise_on_error=False)
    assert not result.success
    assert not any(event.is_step_start and event.step_key == "transform_hourly" for event in result.all_events)
