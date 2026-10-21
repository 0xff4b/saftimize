![Saftimize — optimize your power usage](docs/saftimize_lander.png)

# Saftimize — Optimize your energy usage

Batch data pipeline for the HSLU Data Engineering module (DENG HS26).
It collects Swiss electricity load and price data, stores it raw and curated, and serves it to a dashboard that shows when power is likely to get expensive.

> Status: local midterm implementation includes raw ingestion, hourly electricity transformation, and a daily Dagster job in Docker Compose. Cloud infrastructure, dashboard, and forecasting remain future work. See validation limitations below.

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
3. Transformations resolve electricity revisions, align intervals, and join hourly load and prices (weather remains raw for now)
4. Curated tables are served from PostgreSQL (midterm) and BigQuery (final)
5. Everything runs in Docker Compose locally, cloud resources come from Terraform

## Repository structure

| Directory | Contents |
|---|---|
| `docs/` | Use case, architecture, backlog and other project documentation |
| `src/ingest/` | Source modules, raw storage, hourly transformation, Dagster definitions and CLIs |
| `tests/` | Ingestion, transformation and orchestration tests; opt-in PostgreSQL integration tests |
| `compose.yaml` | PostgreSQL, Dagster code server, webserver, daemon and manual pipeline |
| `docker/` | Persistent Dagster instance and code-location configuration |

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
# Only if no configured .env exists; do not overwrite an existing secret file.
if (-not (Test-Path .env)) { Copy-Item src\.env.example .env }
```

Edit `.env`: supply an ENTSO-E Transparency Platform token with API access enabled, choose a local PostgreSQL password, and put the same password into `DATABASE_URL` (URL-encoded if necessary). Open-Meteo's public non-commercial API needs no key. Do not commit credentials.

```powershell
docker compose up -d --build
# One immediate end-to-end run (ingestion, then transformation):
docker compose run --rm pipeline
```

If credentials are already in `src/.env`, add `--env-file src\.env` immediately after `docker compose`, or move the file to the repository root. Never copy the example over real credentials. Containers use PostgreSQL's internal hostname and `PGPASSWORD`, independent of the host-side `DATABASE_URL`.

Open Dagster at http://localhost:3000. The daemon schedules `daily_energy` at **07:00 UTC daily**, fetching the previous three complete UTC days and then transforming them. The separate code server launches queued runs; the daemon and UI share persistent instance storage. Maximum scheduled concurrency is one. The schedule starts enabled; stop it in the UI if you do not want automatic requests. Run history survives container recreation. This small local instance uses SQLite for orchestration metadata, not as pipeline data storage.

For a historical run, select `daily_energy` in the Dagster Launchpad and use:

```yaml
ops:
  ingest_electricity:
    config:
      start: "2026-10-17"
      end: "2026-10-20"
```

Alternatively, edit `docker/backfill.example.yaml`, rebuild the image, then run `docker compose run --rm pipeline dagster job execute -m ingest.definitions -j daily_energy -c /app/backfill.example.yaml`. The manual container uses temporary Dagster run history; use the UI for persistent history and queued concurrency control. Avoid simultaneous manual and scheduled runs.

`end` is exclusive. Successful ingestion is required before transformation executes. Each step has two orchestration retries (10-second exponential backoff), in addition to the bounded HTTP retries. Historical backfills are explicit date-range runs, not Dagster partition backfills.

For development without the orchestrator:

```powershell
docker compose up -d --wait postgres
uv run --env-file .env saftimize-ingest entsoe
uv run --env-file .env saftimize-transform
```

The default fetches the three complete UTC days before today. Use `entsoe` for load and prices only, or `open-meteo` for weather only. Historical backfills use explicit bounds; `--end` is **exclusive**:

```powershell
uv run --env-file .env saftimize-ingest entsoe --start 2026-10-15 --end 2026-10-18
uv run --env-file .env saftimize-ingest open-meteo --lookback-days 5
```

Each source makes one request per UTC day. The weather default location is Zug (47.1662, 8.5155); override with `--latitude` and `--longitude`. Forecast requests support dates up to 15 days ahead; older backfills use the historical forecast archive (2022 onward, availability depends on model). The default intentionally does not ingest tomorrow's prices: request a future end date explicitly once they are published. Unavailable ENTSO-E data fails the command rather than recording an empty success.

### Raw storage and reruns

`raw.responses` holds the exact HTTP response bytes (`bytea`), source, UTC request bounds, fetch timestamp, endpoint, non-secret request parameters, content type, and SHA-256 hashes. Tokens are never stored. The transformation layer parses electricity intervals without modifying the raw payloads.

The combination of request identity and body hash is unique: an identical rerun adds no row; changed responses are retained as revisions. `fetched_at` records the first retrieval of each distinct version, not every duplicate check. Each response commits independently, so a later failure preserves earlier successful work; rerun the same dates safely. Daily lookback reloads recent windows to catch revisions; older corrections need an explicit backfill. Transient network failures, HTTP 408/429 and selected 5xx responses get four attempts with bounded backoff and `Retry-After` support. Other errors fail immediately with exit code 1 (invalid arguments/configuration: 2).

### Submission and defence

Midterm repository deadline: **22 October 2026, 15:30 Europe/Zurich**. Submit the repository URL and assessed commit hash or release tag, plus the required team/name details, through ILIAS. Grant instructor and peer-review access. Submission is manual; this repository does not upload anything automatically. The defence is ten minutes of questions, with no prepared slides or live demo. Be able to explain incremental lookback, raw revision retention, UTC/DST handling, weighted aggregation, missing coverage, Dagster dependencies and recovery, and the distinction between national load/wholesale prices and household consumption/tariffs.

### Verification

```powershell
docker compose exec postgres psql -U saftimize -d saftimize -c "SELECT source, count(*), min(window_start), max(window_end) FROM raw.responses GROUP BY source;"
docker compose exec postgres psql -U saftimize -d saftimize -c "SELECT source, window_start, fetched_at, octet_length(body) AS bytes FROM raw.responses ORDER BY fetched_at DESC LIMIT 10;"
uv run pytest -q
```

Run the same explicit date range twice: unchanged responses should be reported without increasing row counts. Source revisions may legitimately add rows. ENTSO-E also regenerates document IDs and creation timestamps on requests, so even unchanged measurements can produce new raw bodies; deduplication guarantees byte-identical response deduplication, not one raw row per day. The curated layer resolves these repeated observations. To run the database integration test, set `TEST_DATABASE_URL` to a **disposable test database** before running pytest; the test rolls back its changes. Without it, that test is skipped.

```powershell
$env:TEST_DATABASE_URL = 'postgresql://saftimize:YOUR_PASSWORD@localhost:5432/saftimize_test'
uv run pytest -q
```

Create that separate test database first. `docker compose stop` stops services while retaining data. Do not run `docker compose down -v` unless you intend to delete the raw data and Dagster history.

### Hourly transformation and rationale

`saftimize-transform --start YYYY-MM-DD --end YYYY-MM-DD` rebuilds the requested range in `curated.hourly_energy`. Grain: **one UTC hour per Swiss bidding zone**. The transformation resolves raw response revisions, parses ENTSO-E intervals, calculates duration-weighted hourly load (MW) and price (EUR/MWh), and joins the two series. A full UTC hour needs 3,600 seconds of coverage; incomplete coverage is exposed and the corresponding average is null rather than misleadingly calculated from partial data. Negative prices remain valid.

This supports comparing grid demand with market prices on a consistent time axis. It is more than format conversion: sources can differ in resolution and overlap across request windows. Atomic range replacement makes reruns safe. UTC keys avoid duplicate hours during autumn DST; use `hour_utc AT TIME ZONE 'Europe/Zurich'` only for display. Local calendar days may have 23 or 25 hours. Hourly aggregation intentionally hides intra-hour price variation.

```sql
SELECT hour_utc, hour_utc AT TIME ZONE 'Europe/Zurich' AS swiss_local_hour,
       load_mw, price_eur_mwh, load_coverage_seconds, price_coverage_seconds
FROM curated.hourly_energy
ORDER BY hour_utc DESC
LIMIT 48;
```

```powershell
docker compose exec postgres psql -U saftimize -d saftimize -c "SELECT count(*), min(hour_utc), max(hour_utc) FROM curated.hourly_energy;"
docker compose logs --tail 100 dagster-daemon dagster-code
```

Open-Meteo remains an optional raw-only ingestion command, not a dependency of the assessed electricity job. Terraform, GCS, BigQuery and curated weather are final-milestone work.

## Known limitations (so far)

- Total load is national, not household consumption
- The household meter is not available before the final deadline
- Wholesale prices are not the same as the retail tariff the family pays
- Daily scheduling refreshes historical hours, not an intraday prediction product
- No external alerting or automatic historical catch-up after a long outage; launch explicit date ranges
- Local Dagster metadata uses SQLite and is not a production/high-availability deployment
- Validation on 21 October: Docker image built and services started; the transformation successfully produced 72 fully covered hourly load/price rows for 18–20 October and reran without increasing the row count. All 40 tests passed with a separate PostgreSQL test database. A fresh full scheduled ingestion-to-transformation run still needs confirmation.