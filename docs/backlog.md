# Saftimize project plan and backlog

Dates come from the module description. Repo submission is one week before each oral defence.

## Pitch (Week 3)

- [x] README, dataset and use-case description, Architecture v0.1, backlog in the repo
- [x] Register for ENTSO-E API token and test one request
- [x] Confirm solo team status with the instructor

## Midterm (repo due Thursday 22 Oct 2026, 15:30)

Implementation progress: the manually triggered Docker end-to-end Dagster job passed on 22 October 2026. Run `da2a0739-1019-4a21-97b2-e35047756730` ingested six new ENTSO-E load/price documents and transformed 19–21 October UTC into 72 fully covered hourly rows, with persisted status `SUCCESS`. The earlier transformation rerun retained its row count; all 40 regression tests passed with PostgreSQL integration enabled on 21 October. Automatic schedule firing and independent clean-clone reproduction remain unverified. ENTSO-E regenerates response metadata, so repeated API calls can append raw documents even if measurements are unchanged; byte-identical response deduplication is tested separately.

- [x] Choose orchestrator and write the decision down (Dagster)
- [x] Docker Compose with Postgres, orchestrator and ingestion on one network
- [x] Ingestion module for ENTSO-E load, incremental with lookback
- [x] Ingestion module for ENTSO-E prices
- [x] Raw schema in PostgreSQL, identical response writes are idempotent
- [x] Orchestrated daily job with retries and explicit date-range reruns (manual E2E verified; automatic firing unverified)
- [x] One justified transformation (clean, handle DST, join load and price)
- [x] Setup, run and verification steps in the README, `.env.example` (independent clean-clone reproduction still pending)
- [x] Architecture v0.2
- [ ] Be able to explain every design choice (oral defence is questions only)

### Remaining midterm handover

- [ ] Independently follow the README from a clean clone and verify outputs
- [ ] Observe an automatic daily schedule run (manual job execution already verified)
- [ ] Review and commit the assessed repository state; record its hash or release tag
- [ ] Confirm instructor and assigned peer-review access
- [ ] Submit repository URL, assessed hash/tag and required team/name details through ILIAS by the deadline

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