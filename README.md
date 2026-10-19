![Saftimize — optimize your power usage](docs/saftimize_lander.png)

# Saftimize — Optimize your energy usage

Batch data pipeline for the HSLU Data Engineering module (DENG HS26).
It collects Swiss electricity load and price data, stores it raw and curated, and serves it to a dashboard that shows when power is likely to get expensive.

> Status: ingestion implemented for ENTSO-E load/prices and Open-Meteo, with immutable raw responses in PostgreSQL. Orchestration and curated transformations are not implemented yet.

## Problem and user

My household wants to monitor its electricity usage and know in advance when costs are likely to spike, so that flexible loads (dishwasher, washing machine, EV charging later) can be shifted to cheaper hours.

- **End users:** me and my family
- **Data product:** a curated, queryable table of hourly load and price per hour, plus a dashboard and later a simple price forecast
- Details: [docs/usecase.md](docs/usecase.md)

## Data source (current)

ENTSO-E Transparency Platform, Swiss bidding zone:

- actual total load (usage side)
- day-ahead prices (price side, see open question in the use-case doc)

Our own smart meters are installed in early 2027, after the final deadline. ENTSO-E is the stand-in. The pipeline is designed so the household meter can be added as a second source later without changing the layers behind it.

Open-Meteo provides hourly modelled weather for Zug as a secondary source to explain load and price. Recent dates use the Forecast API; older dates use the Historical Forecast API. This is model output, not measured station observations.

## Architecture

See [docs/architecture.md](docs/architecture.md).

Short version:

1. Orchestrator triggers a daily batch ingestion from the ENTSO-E API
2. Raw responses land in storage (local Postgres first, Google Cloud Storage in the final version)
3. Transformations clean and join load, price and weather
4. Curated tables are served from PostgreSQL (midterm) and BigQuery (final)
5. Everything runs in Docker Compose locally, cloud resources come from Terraform

## Repository structure

| Directory | Contents |
|---|---|
| `docs/` | Use case, architecture, backlog and other project documentation |
| `src/ingest/` | ENTSO-E and Open-Meteo modules, HTTP retries, raw storage and CLI |
| `tests/` | Offline ingestion tests and an opt-in PostgreSQL integration test |
| `compose.yaml` | Local PostgreSQL service with persistent storage |

## Plan

See [docs/backlog.md](docs/backlog.md).

| Milestone | Date | Goal |
|---|---|---|
| Pitch | Week 3 | Direction, README, Architecture v0.1, backlog |
| Midterm | repo due 22 Oct 2026 | Local pipeline: ingestion, Postgres, Docker Compose, orchestrator, one transformation |
| Final | repo due 10 Dec 2026 | Cloud pipeline with Terraform, GCS, BigQuery, data quality checks |

## Setup

Requirements: **uv**, Python 3.13+ (uv can download it), and Docker Desktop running Linux containers. Commands below are PowerShell, run from the repository root.

```powershell
uv sync --locked
Copy-Item src\.env.example .env
```

Edit `.env`: supply an ENTSO-E Transparency Platform token with API access enabled, choose a local PostgreSQL password, and put the same password into `DATABASE_URL` (URL-encoded if necessary). Open-Meteo's public non-commercial API needs no key. Do not commit credentials.

```powershell
docker compose up -d --wait postgres
uv run --env-file .env saftimize-ingest all
```

The default fetches the three complete UTC days before today. Use `entsoe` for load and prices only, or `open-meteo` for weather only. Historical backfills use explicit bounds; `--end` is **exclusive**:

```powershell
uv run --env-file .env saftimize-ingest entsoe --start 2026-10-15 --end 2026-10-18
uv run --env-file .env saftimize-ingest open-meteo --lookback-days 5
```

Each source makes one request per UTC day. The weather default location is Zug (47.1662, 8.5155); override with `--latitude` and `--longitude`. Forecast requests support dates up to 15 days ahead; older backfills use the historical forecast archive (2022 onward, availability depends on model). The default intentionally does not ingest tomorrow's prices: request a future end date explicitly once they are published. Unavailable ENTSO-E data fails the command rather than recording an empty success.

### Raw storage and reruns

`raw.responses` holds the exact HTTP response bytes (`bytea`), source, UTC request bounds, fetch timestamp, endpoint, non-secret request parameters, content type, and SHA-256 hashes. Tokens are never stored. No interval parsing, hourly aggregation, DST conversion, or joining happens yet: those belong to the transformation layer.

The combination of request identity and body hash is unique: an identical rerun adds no row; changed responses are retained as revisions. `fetched_at` records the first retrieval of each distinct version, not every duplicate check. Each response commits independently, so a later failure preserves earlier successful work; rerun the same dates safely. Daily lookback reloads recent windows to catch revisions; older corrections need an explicit backfill. Transient network failures, HTTP 408/429 and selected 5xx responses get four attempts with bounded backoff and `Retry-After` support. Other errors fail immediately with exit code 1 (invalid arguments/configuration: 2).

### Verification

```powershell
docker compose exec postgres psql -U saftimize -d saftimize -c "SELECT source, count(*), min(window_start), max(window_end) FROM raw.responses GROUP BY source;"
docker compose exec postgres psql -U saftimize -d saftimize -c "SELECT source, window_start, fetched_at, octet_length(body) AS bytes FROM raw.responses ORDER BY fetched_at DESC LIMIT 10;"
uv run pytest -q
```

Run the same explicit date range twice: unchanged responses should be reported without increasing row counts. Source revisions may legitimately add rows. To run the database integration test, set `TEST_DATABASE_URL` to a **disposable test database** before running pytest; the test rolls back its changes. Without it, that test is skipped.

```powershell
$env:TEST_DATABASE_URL = 'postgresql://saftimize:YOUR_PASSWORD@localhost:5432/saftimize_test'
uv run pytest -q
```

Create that separate test database first. `docker compose stop` stops PostgreSQL while retaining data. The current Compose file runs only the database; containerized ingestion and an orchestrator remain midterm work.

## Known limitations (so far)

- Total load is national, not household consumption
- The household meter is not available before the final deadline
- Wholesale prices are not the same as the retail tariff the family pays