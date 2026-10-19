from collections.abc import Iterator
from datetime import date, datetime, timezone
from xml.etree.ElementTree import ParseError

from defusedxml import ElementTree
from defusedxml.common import DefusedXmlException
import httpx

from .common import IngestionError, RawResponse, daily_windows, get_response

ENDPOINT = "https://web-api.tp.entsoe.eu/api"
SWISS_AREA = "10YCH-SWISSGRIDZ"


def fetch(
    client: httpx.Client,
    api_key: str,
    start: date,
    end: date,
    *,
    series: str = "load",
    area: str = SWISS_AREA,
) -> Iterator[RawResponse]:
    """Fetch unchanged XML for actual total load or day-ahead prices."""
    if not api_key.strip():
        raise ValueError("ENTSOE_API_KEY is required")
    if series not in {"load", "price"}:
        raise ValueError("series must be load or price")
    for window_start, window_end in daily_windows(start, end):
        parameters = {
            "documentType": "A65" if series == "load" else "A44",
            "outBiddingZone_Domain": area,
            "periodStart": window_start.strftime("%Y%m%d%H%M"),
            "periodEnd": window_end.strftime("%Y%m%d%H%M"),
        }
        if series == "load":
            parameters["processType"] = "A16"
        else:
            parameters["in_Domain"] = parameters.pop("outBiddingZone_Domain")
            parameters["out_Domain"] = area
            parameters["contract_MarketAgreement.type"] = "A01"
        response = get_response(client, ENDPOINT, {**parameters, "securityToken": api_key})
        try:
            root = ElementTree.fromstring(response.content)
        except (ParseError, DefusedXmlException):
            raise IngestionError("ENTSO-E returned invalid or unsafe XML.") from None
        local_name = root.tag.rsplit("}", 1)[-1]
        if local_name == "Acknowledgement_MarketDocument":
            # Do not mark unpublished intervals as successfully ingested.
            raise IngestionError("ENTSO-E returned an acknowledgement, not data; check availability and API access.")
        if not any(element.tag.rsplit("}", 1)[-1] == "TimeSeries" for element in root.iter()):
            raise IngestionError("ENTSO-E response contains no TimeSeries.")
        yield RawResponse(
            source=f"entsoe_{series}",
            window_start=window_start,
            window_end=window_end,
            fetched_at=datetime.now(timezone.utc),
            endpoint=ENDPOINT,
            parameters=parameters,
            content_type=response.headers.get("content-type", "application/xml"),
            body=response.content,
        )
