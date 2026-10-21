from contextlib import nullcontext
from datetime import date, datetime, timedelta, timezone
import os

import psycopg
import pytest

from ingest.common import IngestionError
from ingest.transform import SWISS_AREA, main, transform

UTC = timezone.utc
START = date(2026, 3, 29)
END = date(2026, 3, 30)


class FakeConnection:
    def __init__(self, rows):
        self.rows = rows
        self.deleted = []
        self.inserted = []

    def transaction(self, *args, **kwargs):
        return nullcontext()

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        if normalized.startswith("CREATE SCHEMA"):
            return _Cursor([])
        if normalized.startswith("SELECT id, source, window_start"):
            assert params[0] == ["entsoe_load", "entsoe_price"]
            return _Cursor(self.rows)
        if normalized.startswith("DELETE FROM curated.hourly_energy"):
            self.deleted.append(params)
            return _Cursor([])
        raise AssertionError(normalized)

    def cursor(self):
        return nullcontext(self)

    def executemany(self, sql, seq):
        self.inserted = list(seq)


class _Cursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


def raw_row(identifier, source, start, end, fetched_at, body, parameters=None):
    return (
        identifier,
        source,
        start,
        end,
        fetched_at,
        parameters or {},
        body.encode(),
    )


def load_xml(*, created, revision=1, curve="A01", resolution="PT15M", points, period_start="2026-03-29T00:00Z", period_end="2026-03-30T00:00Z", area=SWISS_AREA):
    body = "".join(points)
    return f"""
    <GL_MarketDocument xmlns="urn:entsoe:load">
      <createdDateTime>{created}</createdDateTime>
      <revisionNumber>{revision}</revisionNumber>
      <TimeSeries>
        <outBiddingZone_Domain.mRID>{area}</outBiddingZone_Domain.mRID>
        <curveType>{curve}</curveType>
        <Period>
          <timeInterval>
            <start>{period_start}</start>
            <end>{period_end}</end>
          </timeInterval>
          <resolution>{resolution}</resolution>
          {body}
        </Period>
      </TimeSeries>
    </GL_MarketDocument>
    """


def price_xml(*, created, revision=1, curve="A01", resolution="PT60M", points, period_start="2026-03-29T00:00Z", period_end="2026-03-30T00:00Z", in_domain=SWISS_AREA, out_domain=SWISS_AREA):
    body = "".join(points)
    return f"""
    <Publication_MarketDocument xmlns="urn:entsoe:price">
      <createdDateTime>{created}</createdDateTime>
      <revisionNumber>{revision}</revisionNumber>
      <TimeSeries>
        <in_Domain.mRID>{in_domain}</in_Domain.mRID>
        <out_Domain.mRID>{out_domain}</out_Domain.mRID>
        <currency_Unit.name>EUR</currency_Unit.name>
        <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
        <curveType>{curve}</curveType>
        <Period>
          <timeInterval>
            <start>{period_start}</start>
            <end>{period_end}</end>
          </timeInterval>
          <resolution>{resolution}</resolution>
          {body}
        </Period>
      </TimeSeries>
    </Publication_MarketDocument>
    """


def sequential_points(values, tag):
    return [f"<Point><position>{index}</position><{tag}>{value}</{tag}></Point>" for index, value in enumerate(values, start=1)]


def block_points(blocks, tag):
    return [
        f"<Point><position>{position}</position>{f'<endPosition>{end}</endPosition>' if end is not None else ''}<{tag}>{value}</{tag}></Point>"
        for position, end, value in blocks
    ]


def test_transform_hourly_weighting_and_visibility_with_gaps():
    day_start = datetime(2026, 3, 29, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)
    rows = [
        raw_row(
            1,
            "entsoe_load",
            day_start,
            day_end,
            day_end,
            load_xml(
                created="2026-03-29T00:10:00Z",
                points=sequential_points([100.0, 200.0, 300.0, 400.0] + [500.0] * 4 + [700.0] * 4 + [900.0] * 4 + [1100.0] * 80, "quantity"),
            ),
        ),
        raw_row(
            2,
            "entsoe_price",
            day_start,
            day_end,
            day_end,
            price_xml(
                created="2026-03-29T00:05:00Z",
                resolution="PT30M",
                points=[
                    "<Point><position>1</position><price.amount>50</price.amount></Point>",
                    "<Point><position>3</position><price.amount>80</price.amount></Point>",
                    "<Point><position>4</position><price.amount>80</price.amount></Point>",
                ],
            ),
        ),
    ]
    connection = FakeConnection(rows)

    assert transform(connection, START, END) == 24

    first, second = connection.inserted[0], connection.inserted[1]
    assert first[0] == day_start
    assert first[2] == pytest.approx(250.0)
    assert first[3] is None
    assert first[4] == 3600
    assert first[5] == 1800
    assert second[0] == day_start + timedelta(hours=1)
    assert second[2] == pytest.approx(500.0)
    assert second[3] == 80.0
    assert second[4] == 3600
    assert second[5] == 3600
    assert all(row[1] == SWISS_AREA for row in connection.inserted)


def test_transform_prefers_latest_response_per_window_and_latest_interval_revision():
    start = datetime(2026, 3, 29, tzinfo=UTC)
    end = start + timedelta(days=1)
    newer_window_response = raw_row(
        20,
        "entsoe_load",
        start,
        end,
        datetime(2026, 3, 29, 3, tzinfo=UTC),
        load_xml(created="2026-03-29T03:00:00Z", points=sequential_points([40.0] * 96, "quantity")),
    )
    older_invalid_window_response = raw_row(
        19,
        "entsoe_load",
        start,
        end,
        datetime(2026, 3, 29, 2, tzinfo=UTC),
        "<broken",
    )
    price_base = raw_row(
        30,
        "entsoe_price",
        start,
        end,
        datetime(2026, 3, 29, 2, tzinfo=UTC),
        price_xml(created="2026-03-29T01:00:00Z", revision=1, points=sequential_points([50.0] * 24, "price.amount")),
    )
    price_newer_revision = raw_row(
        31,
        "entsoe_price",
        start,
        end,
        datetime(2026, 3, 29, 2, 30, tzinfo=UTC),
        price_xml(
            created="2026-03-29T02:00:00Z",
            revision=2,
            points=block_points([(1, 1, 60.0), (2, 24, 50.0)], "price.amount"),
        ),
    )
    connection = FakeConnection([older_invalid_window_response, newer_window_response, price_base, price_newer_revision])

    transform(connection, START, END)

    assert connection.inserted[0][2] == 40.0
    assert connection.inserted[0][3] == 60.0
    assert connection.inserted[1][3] == 50.0


def test_transform_parses_a03_variable_blocks_and_hourly_multiples():
    start = datetime(2026, 10, 25, tzinfo=UTC)
    end = start + timedelta(days=1)
    rows = [
        raw_row(
            1,
            "entsoe_load",
            start,
            end,
            end,
            load_xml(
                created="2026-10-25T00:05:00Z",
                curve="A03",
                resolution="PT30M",
                points=block_points([(1, None, 100.0), (3, None, 200.0), (5, None, 300.0)], "quantity"),
                period_start="2026-10-25T00:00Z",
                period_end="2026-10-25T03:00Z",
            ),
        ),
        raw_row(
            2,
            "entsoe_price",
            start,
            end,
            end,
            price_xml(
                created="2026-10-25T00:01:00Z",
                resolution="PT2H",
                points=sequential_points([80.0, 120.0], "price.amount"),
                period_start="2026-10-25T00:00Z",
                period_end="2026-10-25T04:00Z",
            ),
        ),
    ]
    connection = FakeConnection(rows)

    assert transform(connection, date(2026, 10, 25), date(2026, 10, 26)) == 24

    assert connection.inserted[0][2] == 100.0
    assert connection.inserted[0][3] == 80.0
    assert connection.inserted[0][5] == 3600
    assert connection.inserted[1][2] == 200.0
    assert connection.inserted[1][3] == 80.0
    assert connection.inserted[2][2] == 300.0
    assert connection.inserted[2][3] == 120.0
    assert connection.inserted[2][5] == 3600


def test_transform_rejects_conflicting_same_precedence_overlap():
    start = datetime(2026, 3, 29, tzinfo=UTC)
    end = start + timedelta(days=1)
    rows = [
        raw_row(
            1,
            "entsoe_load",
            start,
            end,
            end,
            load_xml(
                created="2026-03-29T01:00:00Z",
                curve="A03",
                resolution="PT60M",
                points=block_points([(1, 2, 100.0), (2, 3, 200.0)], "quantity"),
                period_end="2026-03-29T03:00Z",
            ),
        )
    ]
    connection = FakeConnection(rows)

    with pytest.raises(IngestionError, match="Conflicting overlapping load intervals"):
        transform(connection, START, END)


def test_transform_filters_non_swiss_price_and_missing_side_remains_visible():
    start = datetime(2026, 3, 29, tzinfo=UTC)
    end = start + timedelta(days=1)
    rows = [
        raw_row(
            1,
            "entsoe_load",
            start,
            end,
            end,
            load_xml(created="2026-03-29T01:00:00Z", points=sequential_points([10.0] * 96, "quantity")),
        ),
        raw_row(
            2,
            "entsoe_price",
            start,
            end,
            end,
            price_xml(created="2026-03-29T01:00:00Z", in_domain="OTHER", out_domain="OTHER", points=sequential_points([99.0] * 24, "price.amount")),
        ),
    ]
    connection = FakeConnection(rows)

    with pytest.raises(IngestionError, match="no matching Swiss intervals"):
        transform(connection, START, END)


def test_main_requires_database_url_and_reports_safe_failure(monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(SystemExit) as error:
        main([])
    assert error.value.code == 2

    monkeypatch.setenv("DATABASE_URL", "postgresql://hidden")

    def connect(*args, **kwargs):
        raise psycopg.OperationalError("secret")

    monkeypatch.setattr("ingest.transform.psycopg.connect", connect)
    assert main(["--start", "2026-03-29", "--end", "2026-03-30"]) == 1
    assert "secret" not in capsys.readouterr().err


@pytest.mark.skipif(not os.environ.get("TEST_DATABASE_URL"), reason="Set TEST_DATABASE_URL to a disposable PostgreSQL database")
def test_transform_integration_transactional_replace_and_revisions():
    start = datetime(2026, 3, 29, tzinfo=UTC)
    end = start + timedelta(days=1)
    load_body = load_xml(created="2026-03-29T00:10:00Z", points=sequential_points([100.0] * 96, "quantity"))
    price_body_old = price_xml(created="2026-03-29T00:05:00Z", revision=1, points=sequential_points([50.0] * 24, "price.amount"))
    price_body_new = price_xml(created="2026-03-29T00:06:00Z", revision=2, points=block_points([(1, 1, 60.0), (2, 24, 50.0)], "price.amount"))
    with psycopg.connect(os.environ["TEST_DATABASE_URL"], autocommit=True) as connection:
        with connection.transaction(force_rollback=True):
            connection.execute(
                """
                CREATE SCHEMA IF NOT EXISTS raw;
                CREATE TABLE IF NOT EXISTS raw.responses (
                    id bigint PRIMARY KEY,
                    source text NOT NULL,
                    window_start timestamptz NOT NULL,
                    window_end timestamptz NOT NULL,
                    fetched_at timestamptz NOT NULL,
                    parameters jsonb NOT NULL,
                    body bytea NOT NULL
                );
                TRUNCATE raw.responses;
                """
            )
            connection.execute(
                "INSERT INTO raw.responses (id, source, window_start, window_end, fetched_at, parameters, body) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)",
                (1, "entsoe_load", start, end, end, "{}", load_body.encode()),
            )
            connection.execute(
                "INSERT INTO raw.responses (id, source, window_start, window_end, fetched_at, parameters, body) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)",
                (2, "entsoe_price", start, end, end, "{}", price_body_old.encode()),
            )
            connection.execute(
                "INSERT INTO raw.responses (id, source, window_start, window_end, fetched_at, parameters, body) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)",
                (3, "entsoe_price", start, end, end + timedelta(minutes=5), "{}", price_body_new.encode()),
            )
            assert transform(connection, START, END) == 24
            rows = connection.execute(
                "SELECT hour_utc, area, load_mw, price_eur_mwh, load_coverage_seconds, price_coverage_seconds FROM curated.hourly_energy ORDER BY hour_utc"
            ).fetchall()
            assert len(rows) == 24
            assert rows[0][1] == SWISS_AREA
            assert rows[0][2] == 100.0
            assert rows[0][3] == 60.0
            assert rows[0][4:] == (3600, 3600)
            assert rows[1][3] == 50.0
            assert transform(connection, START, END) == 24
            repeated = connection.execute(
                "SELECT hour_utc, area, load_mw, price_eur_mwh, load_coverage_seconds, price_coverage_seconds FROM curated.hourly_energy ORDER BY hour_utc"
            ).fetchall()
            assert repeated == rows
            connection.execute(
                "UPDATE raw.responses SET body = %s WHERE id = 3", (b"<broken",)
            )
            connection.execute(
                "UPDATE raw.responses SET body = %s WHERE id = 2", (b"<broken",)
            )
            with pytest.raises(IngestionError):
                transform(connection, START, END)
            assert connection.execute(
                "SELECT hour_utc, area, load_mw, price_eur_mwh, load_coverage_seconds, price_coverage_seconds FROM curated.hourly_energy ORDER BY hour_utc"
            ).fetchall() == rows
