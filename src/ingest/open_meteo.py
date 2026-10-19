from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
import math

import httpx

from .common import IngestionError, RawResponse, daily_windows, get_response

FORECAST_ENDPOINT = "https://api.open-meteo.com/v1/forecast"
HISTORICAL_ENDPOINT = "https://historical-forecast-api.open-meteo.com/v1/forecast"
VARIABLES = ("temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m", "shortwave_radiation")


def fetch(
    client: httpx.Client,
    start: date,
    end: date,
    *,
    latitude: float = 47.1662,
    longitude: float = 8.5155,
) -> Iterator[RawResponse]:
    """Fetch modelled weather for Zug, not station observations."""
    if not math.isfinite(latitude) or not -90 <= latitude <= 90:
        raise ValueError("latitude must be between -90 and 90")
    if not math.isfinite(longitude) or not -180 <= longitude <= 180:
        raise ValueError("longitude must be between -180 and 180")
    today = datetime.now(timezone.utc).date()
    if end > today + timedelta(days=16):
        raise ValueError("weather end exceeds the 16-day forecast horizon")
    if start < date(2022, 1, 1):
        raise ValueError("historical forecast requests must start in 2022 or later")
    for window_start, window_end in daily_windows(start, end):
        endpoint = HISTORICAL_ENDPOINT if window_start.date() < today - timedelta(days=5) else FORECAST_ENDPOINT
        parameters = {
            "latitude": str(latitude),
            "longitude": str(longitude),
            "hourly": ",".join(VARIABLES),
            "timezone": "UTC",
            "start_date": window_start.date().isoformat(),
            "end_date": (window_end.date() - timedelta(days=1)).isoformat(),
        }
        response = get_response(client, endpoint, parameters)
        try:
            payload = response.json()
        except ValueError:
            raise IngestionError("Open-Meteo returned invalid JSON.") from None
        if not isinstance(payload, dict) or payload.get("error"):
            raise IngestionError("Open-Meteo returned an error or unexpected response.")
        hourly = payload.get("hourly")
        expected_times = [(window_start + timedelta(hours=hour)).strftime("%Y-%m-%dT%H:%M") for hour in range(24)]
        if not isinstance(hourly, dict) or hourly.get("time") != expected_times:
            raise IngestionError("Open-Meteo returned missing or unexpected hourly timestamps.")
        for variable in VARIABLES:
            values = hourly.get(variable)
            if not isinstance(values, list) or len(values) != 24:
                raise IngestionError(f"Open-Meteo returned missing or misaligned {variable} values.")
        yield RawResponse(
            source="open_meteo",
            window_start=window_start,
            window_end=window_end,
            fetched_at=datetime.now(timezone.utc),
            endpoint=endpoint,
            parameters=parameters,
            content_type=response.headers.get("content-type", "application/json"),
            body=response.content,
        )
