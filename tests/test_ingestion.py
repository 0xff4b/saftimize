from contextlib import nullcontext
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import json
import os
from unittest.mock import Mock

import httpx
import psycopg
import pytest

from ingest import cli, entsoe, open_meteo, storage
from ingest.common import IngestionError, RawResponse, daily_windows, get_response


XML = b'<GL_MarketDocument xmlns="urn:entsoe:test"><TimeSeries><Period/></TimeSeries></GL_MarketDocument>'
START = date(2026, 1, 1)
END = date(2026, 1, 2)


def client_for(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def weather_payload(day=START):
    start = datetime.combine(day, datetime.min.time())
    return {"hourly": {"time": [(start + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M") for h in range(24)], **{v: [None] * 24 for v in open_meteo.VARIABLES}}}


def record():
    start, end = next(daily_windows(START, END))
    return RawResponse("entsoe_load", start, end, datetime.now(timezone.utc), entsoe.ENDPOINT, {"documentType": "A65"}, "application/xml", XML)


@pytest.mark.parametrize("series,document", [("load", "A65"), ("price", "A44")])
def test_entsoe_parameters_and_unchanged_body(series, document):
    def handler(request):
        params = request.url.params
        assert params["securityToken"] == "secret-token"
        assert params["documentType"] == document
        assert params["periodStart"] == "202601010000"
        assert params["periodEnd"] == "202601020000"
        if series == "load":
            assert params["processType"] == "A16"
            assert params["outBiddingZone_Domain"] == entsoe.SWISS_AREA
        else:
            assert params["in_Domain"] == params["out_Domain"] == entsoe.SWISS_AREA
            assert params["contract_MarketAgreement.type"] == "A01"
        return httpx.Response(200, content=XML)
    with client_for(handler) as client:
        result = list(entsoe.fetch(client, "secret-token", START, END, series=series))
    assert result[0].body == XML
    assert "secret-token" not in repr(result)
    assert "securityToken" not in result[0].parameters


@pytest.mark.parametrize("body", [b"not XML", b"<Acknowledgement_MarketDocument/>", b"<document/>", b'<!DOCTYPE x [<!ENTITY a "unsafe">]><x>&a;</x>'])
def test_entsoe_rejects_invalid_or_no_data_responses(body):
    with client_for(lambda r: httpx.Response(200, content=body)) as client:
        with pytest.raises(IngestionError):
            list(entsoe.fetch(client, "secret", START, END))


@pytest.mark.parametrize("day", [date(2026, 3, 29), date(2026, 10, 25)])
def test_windows_are_24_hours_utc_even_at_dst(day):
    windows = list(daily_windows(day, day + timedelta(days=2)))
    assert len(windows) == 2
    assert all(end - start == timedelta(days=1) for start, end in windows)
    assert windows[0][1] == windows[1][0]


def test_invalid_window():
    with pytest.raises(ValueError):
        list(daily_windows(END, START))


def test_retries_429_and_503(monkeypatch):
    sleep = Mock()
    monkeypatch.setattr("ingest.common.clock.sleep", sleep)
    responses = iter([httpx.Response(429, headers={"Retry-After": "2"}), httpx.Response(503), httpx.Response(200, content=XML)])
    with client_for(lambda r: next(responses)) as client:
        assert get_response(client, entsoe.ENDPOINT, {}).content == XML
    assert [call.args[0] for call in sleep.call_args_list] == [2, 2]


def test_network_retry_failure_does_not_expose_token(monkeypatch):
    monkeypatch.setattr("ingest.common.clock.sleep", lambda _: None)
    def handler(request):
        raise httpx.ConnectError(str(request.url), request=request)
    with client_for(handler) as client:
        with pytest.raises(IngestionError) as error:
            get_response(client, entsoe.ENDPOINT, {"securityToken": "secret-token"}, attempts=2)
    assert "secret-token" not in str(error.value)


def test_auth_failure_is_not_retried():
    handler = Mock(return_value=httpx.Response(401))
    with client_for(handler) as client:
        with pytest.raises(IngestionError, match="401"):
            get_response(client, entsoe.ENDPOINT, {"securityToken": "secret-token"})
    assert handler.call_count == 1


@pytest.mark.parametrize("historical", [True, False])
def test_weather_endpoint_parameters_and_raw_body(historical):
    day = START if historical else datetime.now(timezone.utc).date()
    body = json.dumps(weather_payload(day)).encode()
    def handler(request):
        assert str(request.url).startswith(open_meteo.HISTORICAL_ENDPOINT if historical else open_meteo.FORECAST_ENDPOINT)
        assert request.url.params["timezone"] == "UTC"
        assert request.url.params["start_date"] == request.url.params["end_date"] == day.isoformat()
        return httpx.Response(200, content=body)
    with client_for(handler) as client:
        result = list(open_meteo.fetch(client, day, day + timedelta(days=1)))
    assert result[0].body == body


@pytest.mark.parametrize("payload", [{"error": True}, {}, [], {"hourly": {"time": []}}])
def test_weather_rejects_bad_shapes(payload):
    with client_for(lambda r: httpx.Response(200, json=payload)) as client:
        with pytest.raises(IngestionError):
            list(open_meteo.fetch(client, START, END))


def test_weather_rejects_misaligned_variable():
    payload = weather_payload()
    payload["hourly"]["temperature_2m"] = []
    with client_for(lambda r: httpx.Response(200, json=payload)) as client:
        with pytest.raises(IngestionError, match="temperature_2m"):
            list(open_meteo.fetch(client, START, END))


@pytest.mark.parametrize("latitude,longitude", [(91, 8), (47, 181), (float("nan"), 8)])
def test_invalid_coordinates(latitude, longitude):
    with client_for(lambda r: pytest.fail("must not request")) as client:
        with pytest.raises(ValueError):
            list(open_meteo.fetch(client, START, END, latitude=latitude, longitude=longitude))


def test_storage_parameterization_and_hashing():
    connection = Mock()
    connection.transaction.side_effect = lambda: nullcontext()
    connection.execute.return_value.fetchone.return_value = (1,)
    first = record()
    assert storage.save(connection, first)
    sql, params = connection.execute.call_args.args
    assert "ON CONFLICT" in sql
    assert params[7] == XML
    first_hashes = params[-2:]
    storage.save(connection, replace(first, fetched_at=first.fetched_at + timedelta(hours=1)))
    assert connection.execute.call_args.args[1][-2:] == first_hashes
    storage.save(connection, replace(first, body=XML + b"\n"))
    revised = connection.execute.call_args.args[1][-2:]
    assert revised[0] != first_hashes[0]
    assert revised[1] == first_hashes[1]
    connection.execute.return_value.fetchone.return_value = None
    assert not storage.save(connection, first)


def test_cli_missing_configuration(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(SystemExit) as error:
        cli.main(["all"])
    assert error.value.code == 2


def test_cli_runs_all_sources(monkeypatch, capsys):
    monkeypatch.setenv("DATABASE_URL", "postgresql://test")
    monkeypatch.setenv("ENTSOE_API_KEY", "secret-token")
    connection = Mock()
    monkeypatch.setattr(cli.psycopg, "connect", lambda *a, **k: nullcontext(connection))
    monkeypatch.setattr(storage, "initialize", Mock())
    save = Mock(return_value=True)
    monkeypatch.setattr(storage, "save", save)
    monkeypatch.setattr(entsoe, "fetch", lambda *a, **k: iter([record()]))
    monkeypatch.setattr(open_meteo, "fetch", lambda *a, **k: iter([record()]))
    assert cli.main(["all", "--start", "2026-01-01", "--end", "2026-01-02"]) == 0
    assert save.call_count == 3
    assert "3 new" in capsys.readouterr().out


def test_cli_database_failure_is_safe(monkeypatch, capsys):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:secret-password@localhost/db")
    def connect(*args, **kwargs):
        raise psycopg.OperationalError("secret-password")
    monkeypatch.setattr(cli.psycopg, "connect", connect)
    assert cli.main(["open-meteo"]) == 1
    assert "secret-password" not in capsys.readouterr().err


@pytest.mark.skipif(not os.environ.get("TEST_DATABASE_URL"), reason="Set TEST_DATABASE_URL to a disposable PostgreSQL database")
def test_postgres_roundtrip_idempotency_and_revisions():
    with psycopg.connect(os.environ["TEST_DATABASE_URL"], autocommit=True) as connection:
        # Roll back the complete test, including schema changes, after assertions.
        with connection.transaction(force_rollback=True):
            storage.initialize(connection)
            connection.execute("TRUNCATE raw.responses")
            first = record()
            assert storage.save(connection, first)
            assert not storage.save(connection, first)
            assert storage.save(connection, replace(first, body=XML + b"\n"))
            rows = connection.execute("SELECT body FROM raw.responses ORDER BY id").fetchall()
            assert [row[0] for row in rows] == [XML, XML + b"\n"]
