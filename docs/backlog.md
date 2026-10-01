# Project plan and backlog

Dates come from the module description. Repo submission is one week before each oral defence.

## Pitch (Week 3)

- [x] README, dataset and use-case description, Architecture v0.1, backlog in the repo
- [x] Register for ENTSO-E API token and test one request
- [x] Confirm solo team status with the instructor

## Midterm (repo due Thursday 22 Oct 2026, 15:30)

- [ ] Choose orchestrator and write the decision down
- [ ] Docker Compose with Postgres, orchestrator and ingestion on one network
- [ ] Ingestion module for ENTSO-E load, incremental with lookback
- [ ] Ingestion module for ENTSO-E prices
- [ ] Raw schema in PostgreSQL, loads are idempotent
- [ ] Orchestrated daily DAG with retries and a backfill command
- [ ] One justified transformation (clean, handle DST, join load and price)
- [ ] Setup, run and verification steps in the README, `.env.example`
- [ ] Architecture v0.2
- [ ] Be able to explain every design choice (oral defence is questions only)

## Final (repo due Thursday 10 Dec 2026, 20:00)

- [ ] Terraform for GCS bucket and BigQuery dataset
- [ ] Ingestion writes straight to GCS, no local staging in the production path
- [ ] Transformations read from the lake and write curated BigQuery tables
- [ ] Analytical model with stated grain per table, partitioning and clustering reasoning
- [ ] Data-quality checks, failure handling, safe reruns
- [ ] Weather source added
- [ ] Verification queries and known limitations documented
- [ ] Final architecture and its evolution from v0.1

## Stretch (only if time is left)

- [ ] Simple hourly price forecast
- [ ] Optimization agent that suggests cheap hours for flexible appliances
- [ ] Household meter as an additional source (after the module, early 2027)

## Peer review deadlines

- Midterm reviews: 12 Nov 2026
- Final reviews: 31 Dec 2026