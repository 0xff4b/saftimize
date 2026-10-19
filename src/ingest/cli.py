import argparse
from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
import os
import sys

import httpx
import psycopg

from . import entsoe, open_meteo, storage
from .common import IngestionError


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest unchanged source responses into PostgreSQL.")
    parser.add_argument("source", choices=("entsoe", "open-meteo", "all"))
    parser.add_argument("--start", type=date.fromisoformat, help="First UTC date, inclusive (YYYY-MM-DD)")
    parser.add_argument("--end", type=date.fromisoformat, help="Last UTC date, exclusive; defaults to today")
    parser.add_argument("--lookback-days", type=int, default=3, help="Days before end when start is omitted (default: 3)")
    parser.add_argument("--latitude", type=float, default=47.1662)
    parser.add_argument("--longitude", type=float, default=8.5155)
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
    api_key = os.environ.get("ENTSOE_API_KEY", "")
    if args.source in {"entsoe", "all"} and not api_key.strip():
        parser.error("ENTSOE_API_KEY is required for ENTSO-E")
    inserted = unchanged = 0
    try:
        with psycopg.connect(database_url, autocommit=True, connect_timeout=10) as connection:
            storage.initialize(connection)
            with httpx.Client(headers={"User-Agent": "saftimize/0.1"}, follow_redirects=False) as client:
                batches = []
                if args.source in {"entsoe", "all"}:
                    batches.extend(entsoe.fetch(client, api_key, start, end, series=series) for series in ("load", "price"))
                if args.source in {"open-meteo", "all"}:
                    batches.append(open_meteo.fetch(client, start, end, latitude=args.latitude, longitude=args.longitude))
                for batch in batches:
                    for record in batch:
                        if storage.save(connection, record):
                            inserted += 1
                        else:
                            unchanged += 1
                        print(f"{record.source}: {record.window_start.date()} stored")
    except (IngestionError, ValueError) as error:
        print(f"Ingestion failed: {error}", file=sys.stderr)
        return 1
    except psycopg.Error:
        print("Ingestion failed: PostgreSQL connection or write error; check database configuration.", file=sys.stderr)
        return 1
    print(f"Complete: {inserted} new raw responses, {unchanged} unchanged responses.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
