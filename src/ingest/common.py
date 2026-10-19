from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from email.utils import parsedate_to_datetime
import time as clock

import httpx


class IngestionError(Exception):
    """A source or storage failure safe to display without credentials."""


@dataclass(frozen=True)
class RawResponse:
    source: str
    window_start: datetime
    window_end: datetime
    fetched_at: datetime
    endpoint: str
    parameters: dict[str, str]
    content_type: str
    body: bytes


def daily_windows(start: date, end: date) -> Iterator[tuple[datetime, datetime]]:
    if start >= end:
        raise ValueError("start must be before end (end is exclusive)")
    while start < end:
        following = start + timedelta(days=1)
        yield (
            datetime.combine(start, time.min, timezone.utc),
            datetime.combine(following, time.min, timezone.utc),
        )
        start = following


def _retry_delay(response: httpx.Response | None, attempt: int) -> float:
    if response is not None:
        value = response.headers.get("Retry-After", "")
        try:
            return min(120.0, max(0.0, float(value)))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
                return min(120.0, max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds()))
            except (ValueError, TypeError, OverflowError):
                pass
    return float(2**attempt)


def get_response(
    client: httpx.Client,
    endpoint: str,
    parameters: Mapping[str, str],
    *,
    attempts: int = 4,
) -> httpx.Response:
    """Retry transient failures without exposing URLs containing API tokens."""
    if attempts < 1:
        raise ValueError("attempts must be positive")
    for attempt in range(attempts):
        response = None
        try:
            response = client.get(endpoint, params=parameters, timeout=60)
        except httpx.TransportError:
            if attempt == attempts - 1:
                raise IngestionError("Source request failed after retries (network/timeout).") from None
        else:
            if response.is_success:
                return response
            if response.status_code not in {408, 429, 500, 502, 503, 504}:
                raise IngestionError(f"Source returned HTTP {response.status_code}; check credentials and request settings.")
            if attempt == attempts - 1:
                raise IngestionError(f"Source returned HTTP {response.status_code} after retries.")
        clock.sleep(_retry_delay(response, attempt))
    raise AssertionError("unreachable")
