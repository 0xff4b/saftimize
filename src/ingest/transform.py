from __future__ import annotations

import argparse
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import math
import os
import re
import sys
from xml.etree.ElementTree import Element, ParseError

from defusedxml import ElementTree
from defusedxml.common import DefusedXmlException
import psycopg

from .common import IngestionError

SWISS_AREA = "10YCH-SWISSGRIDZ"
_SUPPORTED_SOURCES = ("entsoe_load", "entsoe_price")
_HOUR = timedelta(hours=1)
_DURATION_MINUTES = re.compile(r"PT(\d+)M")
_DURATION_HOURS = re.compile(r"PT(\d+)H")
_CURATED_SQL = """
CREATE SCHEMA IF NOT EXISTS curated;
CREATE TABLE IF NOT EXISTS curated.hourly_energy (
    hour_utc timestamptz NOT NULL,
    area text NOT NULL,
    load_mw double precision NULL,
    price_eur_mwh double precision NULL,
    load_coverage_seconds integer NOT NULL,
    price_coverage_seconds integer NOT NULL,
    PRIMARY KEY (hour_utc, area),
    CHECK (load_coverage_seconds BETWEEN 0 AND 3600),
    CHECK (price_coverage_seconds BETWEEN 0 AND 3600)
);
"""


@dataclass(frozen=True)
class _StoredResponse:
    id: int
    source: str
    window_start: datetime
    window_end: datetime
    fetched_at: datetime
    parameters: Mapping[str, object]
    body: bytes


@dataclass(frozen=True)
class _DocumentMeta:
    source: str
    response_id: int
    fetched_at: datetime
    document_created_at: datetime
    revision_number: int

    @property
    def precedence(self) -> tuple[datetime, int, datetime, int]:
        return (self.document_created_at, self.revision_number, self.fetched_at, self.response_id)


@dataclass(frozen=True)
class _Interval:
    start: datetime
    end: datetime
    value: float
    meta: _DocumentMeta


@dataclass(frozen=True)
class _HourlyRow:
    hour_utc: datetime
    area: str
    load_mw: float | None
    price_eur_mwh: float | None
    load_coverage_seconds: int
    price_coverage_seconds: int


def _utc_midnight(day: date) -> datetime:
    return datetime.combine(day, time.min, timezone.utc)


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _children(element: Element, name: str) -> list[Element]:
    return [child for child in element if _local_name(child.tag) == name]


def _first_child(element: Element, name: str) -> Element | None:
    for child in element:
        if _local_name(child.tag) == name:
            return child
    return None


def _child_text(element: Element, name: str) -> str:
    child = _first_child(element, name)
    if child is None or child.text is None:
        return ""
    return child.text.strip()


def _parse_datetime(value: str, field_name: str) -> datetime:
    if not value:
        raise IngestionError(f"ENTSO-E response is missing {field_name}.")
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise IngestionError(f"ENTSO-E response has invalid {field_name}.") from None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def _parse_optional_datetime(value: str, field_name: str) -> datetime | None:
    if not value:
        return None
    return _parse_datetime(value, field_name)


def _parse_int(value: str, field_name: str, *, default: int | None = None) -> int:
    if not value:
        if default is None:
            raise IngestionError(f"ENTSO-E response is missing {field_name}.")
        return default
    try:
        return int(value)
    except ValueError:
        raise IngestionError(f"ENTSO-E response has invalid {field_name}.") from None


def _parse_value(value: str, field_name: str) -> float:
    if not value:
        raise IngestionError(f"ENTSO-E response is missing {field_name}.")
    try:
        return float(value)
    except ValueError:
        raise IngestionError(f"ENTSO-E response has invalid {field_name}.") from None


def _parse_resolution(value: str) -> timedelta:
    match = _DURATION_MINUTES.fullmatch(value)
    if match:
        minutes = int(match.group(1))
        if minutes not in {15, 30, 60}:
            raise IngestionError(f"Unsupported ENTSO-E resolution {value}.")
        return timedelta(minutes=minutes)
    match = _DURATION_HOURS.fullmatch(value)
    if match:
        hours = int(match.group(1))
        if hours < 1:
            raise IngestionError(f"Unsupported ENTSO-E resolution {value}.")
        return timedelta(hours=hours)
    raise IngestionError(f"Unsupported ENTSO-E resolution {value}.")


def _series_matches_area(series: Element, source: str, area: str) -> bool:
    text_by_name = {
        _local_name(child.tag): child.text.strip()
        for child in series
        if child.text is not None and child.text.strip()
    }
    if source == "entsoe_load":
        candidates = [
            text_by_name[name]
            for name in ("outBiddingZone_Domain.mRID", "biddingZone_Domain.mRID", "area_Domain.mRID", "out_Domain.mRID")
            if name in text_by_name
        ]
        return bool(candidates) and area in candidates
    in_domain = text_by_name.get("in_Domain.mRID")
    out_domain = text_by_name.get("out_Domain.mRID")
    if not in_domain and not out_domain:
        return False
    if any(value != area for value in (in_domain, out_domain) if value):
        return False
    currency = text_by_name.get("currency_Unit.name")
    if currency and currency != "EUR":
        return False
    price_unit = text_by_name.get("price_Measure_Unit.name")
    return not price_unit or price_unit.upper() == "MWH"


def _interval_from_positions(
    period_start: datetime,
    period_end: datetime,
    resolution: timedelta,
    start_position: int,
    end_position: int,
    value: float,
    meta: _DocumentMeta,
) -> _Interval:
    if start_position < 1 or end_position < start_position:
        raise IngestionError("ENTSO-E point positions are invalid.")
    start = period_start + (start_position - 1) * resolution
    end = period_start + end_position * resolution
    if start < period_start or end > period_end or start >= end:
        raise IngestionError("ENTSO-E points do not fit inside their declared period.")
    return _Interval(start=start, end=end, value=value, meta=meta)


def _point_value(point: Element, source: str) -> float:
    field_name = "quantity" if source == "entsoe_load" else "price.amount"
    return _parse_value(_child_text(point, field_name), field_name)


def _sequential_intervals(
    points: Sequence[Element],
    period_start: datetime,
    period_end: datetime,
    resolution: timedelta,
    meta: _DocumentMeta,
    source: str,
) -> list[_Interval]:
    intervals: list[_Interval] = []
    for point in points:
        start_position_text = _child_text(point, "startPosition")
        end_position_text = _child_text(point, "endPosition")
        if start_position_text or end_position_text:
            start_position = _parse_int(start_position_text or _child_text(point, "position"), "startPosition")
            end_position = _parse_int(end_position_text or start_position_text or _child_text(point, "position"), "endPosition")
        else:
            start_position = _parse_int(_child_text(point, "position"), "position")
            end_position = start_position
        intervals.append(
            _interval_from_positions(
                period_start,
                period_end,
                resolution,
                start_position,
                end_position,
                _point_value(point, source),
                meta,
            )
        )
    return intervals


def _variable_block_intervals(
    points: Sequence[Element],
    period_start: datetime,
    period_end: datetime,
    resolution: timedelta,
    meta: _DocumentMeta,
    source: str,
) -> list[_Interval]:
    explicit_bounds = any(_child_text(point, "startPosition") or _child_text(point, "endPosition") for point in points)
    if explicit_bounds:
        blocks: list[tuple[int, int | None, float]] = []
        for point in points:
            start_position = _parse_int(_child_text(point, "startPosition") or _child_text(point, "position"), "startPosition")
            end_position_text = _child_text(point, "endPosition")
            end_position = _parse_int(end_position_text, "endPosition") if end_position_text else None
            blocks.append((start_position, end_position, _point_value(point, source)))
        blocks.sort(key=lambda block: block[0])
        intervals: list[_Interval] = []
        for index, (start_position, end_position, value) in enumerate(blocks):
            if end_position is None:
                if index + 1 < len(blocks):
                    end_position = blocks[index + 1][0] - 1
                else:
                    covered = (period_end - period_start) / resolution
                    if covered != int(covered):
                        raise IngestionError("ENTSO-E variable blocks do not align with their resolution.")
                    end_position = int(covered)
            intervals.append(_interval_from_positions(period_start, period_end, resolution, start_position, end_position, value, meta))
        return intervals
    starts = sorted(
        (_parse_int(_child_text(point, "position"), "position"), _point_value(point, source))
        for point in points
    )
    intervals = []
    for index, (start_position, value) in enumerate(starts):
        next_start = starts[index + 1][0] if index + 1 < len(starts) else None
        if next_start is None:
            covered = (period_end - period_start) / resolution
            if covered != int(covered):
                raise IngestionError("ENTSO-E variable blocks do not align with their resolution.")
            end_position = int(covered)
        else:
            end_position = next_start - 1
        intervals.append(_interval_from_positions(period_start, period_end, resolution, start_position, end_position, value, meta))
    return intervals


def _period_intervals(period: Element, meta: _DocumentMeta, source: str, curve_type: str) -> list[_Interval]:
    time_interval = _first_child(period, "timeInterval")
    if time_interval is None:
        raise IngestionError("ENTSO-E period is missing timeInterval.")
    period_start = _parse_datetime(_child_text(time_interval, "start"), "timeInterval.start")
    period_end = _parse_datetime(_child_text(time_interval, "end"), "timeInterval.end")
    if period_end <= period_start:
        raise IngestionError("ENTSO-E period end must be after period start.")
    resolution = _parse_resolution(_child_text(period, "resolution"))
    points = _children(period, "Point")
    if not points:
        return []
    if curve_type == "A01":
        return _sequential_intervals(points, period_start, period_end, resolution, meta, source)
    if curve_type == "A03":
        return _variable_block_intervals(points, period_start, period_end, resolution, meta, source)
    raise IngestionError(f"Unsupported ENTSO-E curveType {curve_type}.")


def _parse_response(response: _StoredResponse, area: str = SWISS_AREA) -> list[_Interval]:
    try:
        root = ElementTree.fromstring(response.body)
    except (ParseError, DefusedXmlException):
        raise IngestionError("ENTSO-E raw response contains invalid or unsafe XML.") from None
    if _local_name(root.tag) == "Acknowledgement_MarketDocument":
        raise IngestionError("ENTSO-E raw response is an acknowledgement, not data.")
    created_at = _parse_optional_datetime(_child_text(root, "createdDateTime"), "createdDateTime") or datetime.min.replace(tzinfo=timezone.utc)
    revision_number = _parse_int(_child_text(root, "revisionNumber"), "revisionNumber", default=0)
    meta = _DocumentMeta(
        source=response.source,
        response_id=response.id,
        fetched_at=response.fetched_at,
        document_created_at=created_at,
        revision_number=revision_number,
    )
    intervals: list[_Interval] = []
    for series in root.iter():
        if _local_name(series.tag) != "TimeSeries":
            continue
        if not _series_matches_area(series, response.source, area):
            continue
        curve_type = _child_text(series, "curveType") or "A01"
        for period in _children(series, "Period"):
            intervals.extend(_period_intervals(period, meta, response.source, curve_type))
    if not intervals:
        raise IngestionError("ENTSO-E raw response contains no matching Swiss intervals.")
    return intervals


def _collect_intervals_by_source(responses: Iterable[_StoredResponse]) -> dict[str, list[_Interval]]:
    ordered = sorted(
        responses,
        key=lambda row: (row.source, row.window_start, row.window_end, row.fetched_at, row.id),
        reverse=True,
    )
    chosen_keys: set[tuple[str, datetime, datetime]] = set()
    intervals_by_source: dict[str, list[_Interval]] = defaultdict(list)
    errors: dict[tuple[str, datetime, datetime], IngestionError] = {}
    for response in ordered:
        key = (response.source, response.window_start, response.window_end)
        if key in chosen_keys:
            continue
        try:
            intervals = _parse_response(response)
        except IngestionError as error:
            errors.setdefault(key, error)
            continue
        intervals_by_source[response.source].extend(intervals)
        chosen_keys.add(key)
    unresolved = sorted(set(errors) - chosen_keys)
    if unresolved:
        source, window_start, window_end = unresolved[0]
        error = errors[(source, window_start, window_end)]
        raise IngestionError(
            f"No complete raw response available for {source} {window_start.isoformat()} to {window_end.isoformat()}: {error}"
        )
    return intervals_by_source


def _clip_intervals(intervals: Iterable[_Interval], start: datetime, end: datetime) -> list[_Interval]:
    clipped: list[_Interval] = []
    for interval in intervals:
        clipped_start = max(interval.start, start)
        clipped_end = min(interval.end, end)
        if clipped_start < clipped_end:
            clipped.append(_Interval(start=clipped_start, end=clipped_end, value=interval.value, meta=interval.meta))
    return clipped


def _same_value(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-9)


def _resolve_intervals(intervals: Iterable[_Interval], start: datetime, end: datetime, *, label: str) -> list[_Interval]:
    clipped = _clip_intervals(intervals, start, end)
    if not clipped:
        return []
    boundaries = sorted({start, end, *(interval.start for interval in clipped), *(interval.end for interval in clipped)})
    resolved: list[_Interval] = []
    for segment_start, segment_end in zip(boundaries, boundaries[1:]):
        if segment_start >= segment_end:
            continue
        covering = [
            interval
            for interval in clipped
            if interval.start <= segment_start and interval.end >= segment_end
        ]
        if not covering:
            continue
        winner = max(covering, key=lambda interval: interval.meta.precedence)
        top_covering = [interval for interval in covering if interval.meta.precedence == winner.meta.precedence]
        if any(not _same_value(interval.value, winner.value) for interval in top_covering[1:]):
            raise IngestionError(f"Conflicting overlapping {label} intervals share the same revision precedence.")
        if resolved and resolved[-1].end == segment_start and _same_value(resolved[-1].value, winner.value) and resolved[-1].meta.precedence == winner.meta.precedence:
            resolved[-1] = _Interval(start=resolved[-1].start, end=segment_end, value=resolved[-1].value, meta=resolved[-1].meta)
        else:
            resolved.append(_Interval(start=segment_start, end=segment_end, value=winner.value, meta=winner.meta))
    return resolved


def _aggregate_metric(intervals: Iterable[_Interval]) -> dict[datetime, tuple[float, int]]:
    sums: dict[datetime, float] = defaultdict(float)
    coverages: dict[datetime, int] = defaultdict(int)
    for interval in intervals:
        hour = interval.start.replace(minute=0, second=0, microsecond=0)
        while hour < interval.end:
            overlap_start = max(interval.start, hour)
            overlap_end = min(interval.end, hour + _HOUR)
            seconds = int((overlap_end - overlap_start).total_seconds())
            if seconds > 0:
                sums[hour] += interval.value * seconds
                coverages[hour] += seconds
            hour += _HOUR
    return {hour: (sums[hour], coverages[hour]) for hour in set(sums) | set(coverages)}


def _mean_if_complete(summary: tuple[float, int] | None) -> tuple[float | None, int]:
    if summary is None:
        return None, 0
    weighted_sum, coverage = summary
    if coverage != 3600:
        return None, coverage
    return weighted_sum / coverage, coverage


def _build_hour_rows(load_intervals: Iterable[_Interval], price_intervals: Iterable[_Interval], start: datetime, end: datetime) -> list[_HourlyRow]:
    load_summary = _aggregate_metric(load_intervals)
    price_summary = _aggregate_metric(price_intervals)
    rows: list[_HourlyRow] = []
    hour = start
    while hour < end:
        load_mw, load_coverage = _mean_if_complete(load_summary.get(hour))
        price_eur_mwh, price_coverage = _mean_if_complete(price_summary.get(hour))
        rows.append(
            _HourlyRow(
                hour_utc=hour,
                area=SWISS_AREA,
                load_mw=load_mw,
                price_eur_mwh=price_eur_mwh,
                load_coverage_seconds=load_coverage,
                price_coverage_seconds=price_coverage,
            )
        )
        hour += _HOUR
    return rows


def _load_responses(connection: psycopg.Connection, start: datetime, end: datetime) -> list[_StoredResponse]:
    rows = connection.execute(
        """
        SELECT id, source, window_start, window_end, fetched_at, parameters, body
        FROM raw.responses
        WHERE source = ANY(%s)
          AND window_end > %s
          AND window_start < %s
        ORDER BY source, window_start, window_end, fetched_at DESC, id DESC
        """,
        (list(_SUPPORTED_SOURCES), start, end),
    ).fetchall()
    return [
        _StoredResponse(
            id=row[0],
            source=row[1],
            window_start=row[2],
            window_end=row[3],
            fetched_at=row[4],
            parameters=row[5],
            body=row[6],
        )
        for row in rows
    ]


def transform(connection: psycopg.Connection, start: date, end: date) -> int:
    if start >= end:
        raise ValueError("start must be before end (end is exclusive)")
    start_utc = _utc_midnight(start)
    end_utc = _utc_midnight(end)
    with connection.transaction():
        connection.execute(_CURATED_SQL)
        responses = _load_responses(connection, start_utc, end_utc)
        intervals_by_source = _collect_intervals_by_source(responses)
        load_intervals = _resolve_intervals(intervals_by_source.get("entsoe_load", ()), start_utc, end_utc, label="load")
        price_intervals = _resolve_intervals(intervals_by_source.get("entsoe_price", ()), start_utc, end_utc, label="price")
        rows = _build_hour_rows(load_intervals, price_intervals, start_utc, end_utc)
        connection.execute(
            "DELETE FROM curated.hourly_energy WHERE area = %s AND hour_utc >= %s AND hour_utc < %s",
            (SWISS_AREA, start_utc, end_utc),
        )
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO curated.hourly_energy
                    (hour_utc, area, load_mw, price_eur_mwh, load_coverage_seconds, price_coverage_seconds)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (hour_utc, area) DO UPDATE
                SET load_mw = EXCLUDED.load_mw,
                    price_eur_mwh = EXCLUDED.price_eur_mwh,
                    load_coverage_seconds = EXCLUDED.load_coverage_seconds,
                    price_coverage_seconds = EXCLUDED.price_coverage_seconds
                """,
                [
                    (
                        row.hour_utc,
                        row.area,
                        row.load_mw,
                        row.price_eur_mwh,
                        row.load_coverage_seconds,
                        row.price_coverage_seconds,
                    )
                    for row in rows
                ],
            )
    return len(rows)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Transform local ENTSO-E XML into curated Swiss hourly energy rows.")
    parser.add_argument("--start", type=date.fromisoformat, help="First UTC date, inclusive (YYYY-MM-DD)")
    parser.add_argument("--end", type=date.fromisoformat, help="Last UTC date, exclusive; defaults to today")
    parser.add_argument("--lookback-days", type=int, default=3, help="Days before end when start is omitted (default: 3)")
    args = parser.parse_args(argv)
    end = args.end or datetime.now(timezone.utc).date()
    if args.lookback_days < 1:
        parser.error("--lookback-days must be positive")
    start = args.start or end - timedelta(days=args.lookback_days)
    if start >= end:
        parser.error("--start must precede --end")
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        parser.error("DATABASE_URL is required")
    try:
        with psycopg.connect(database_url, connect_timeout=10) as connection:
            row_count = transform(connection, start, end)
    except (IngestionError, ValueError) as error:
        print(f"Transformation failed: {error}", file=sys.stderr)
        return 1
    except psycopg.Error:
        print("Transformation failed: PostgreSQL connection or write error; check database configuration.", file=sys.stderr)
        return 1
    print(f"Complete: replaced {row_count} hourly rows for {SWISS_AREA}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
