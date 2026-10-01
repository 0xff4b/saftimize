# Dataset and use case

## Use case

**Monitor electricity usage and anticipate cost spikes.**

The user is a household (me and my family). The questions the data product should answer:

1. How much electricity is being used right now compared with normal for this hour, weekday and season?
2. Which upcoming hours are likely to be expensive?
3. When would be a good time to run flexible appliances?

Output (data product): a curated hourly table with load, price and weather features, a dashboard on top of it, and later a price forecast and a simple optimization suggestion (stretch goal, see backlog).

## Why ENTSO-E for now

The household smart meters are installed in early 2027, which is after the final submission. The instructor asked for real-world data with real anomalies, so ENTSO-E replaces the meter for the assessed version. The ingestion layer is built per source, so the meter can be added as another source later.

## Source 1: ENTSO-E Transparency Platform

| Item | Description |
|---|---|
| Provenance | European Network of Transmission System Operators for Electricity, public transparency platform |
| Access | REST API with a personal security token (free registration). Client libraries exist, for example `entsoe-py` |
| Format | XML responses |
| Region | Switzerland bidding zone (verify the exact area code during setup) |
| Datasets | Actual total load (usage side). Day-ahead prices (price side, to be confirmed) |
| Resolution | Hourly or 15 minutes depending on dataset and period (verify) |
| Update frequency | Load is published close to real time, day-ahead prices once per day for the next day |
| Volume | Small. A few thousand rows per month per series |

### Data-quality risks

- Missing intervals and delayed publication
- Revised values (later corrections of earlier values)
- Resolution changes over time (hourly vs 15 minutes)
- Time zone and daylight saving transitions (23 and 25 hour days)
- API rate limits and occasional errors
- XML structure differs between document types

These risks are the reason the source fits the module: they force real cleaning, deduplication and idempotent reruns.

## Source 2: weather (planned)

A Swiss weather source for the Zug area, for example MeteoSwiss open data or Open-Meteo. Final choice is open and depends on API stability and history length. Used to explain load (heating, cooling) and price (wind, sun).

## Source 3: household meter (future)

IoT-readable smart meter, early 2027. Out of scope for assessment. Kept in mind for the schema: a household usage table with the same hourly grain.

## Open questions

1. **Price side:** which ENTSO-E price series maps best to what the family pays? Wholesale day-ahead prices follow the market, the retail tariff does not follow them hour by hour. The dashboard should say clearly that price is a market signal, not the bill.
2. **Meaning of national load:** it works as a signal for grid stress and price pressure, not as household usage. The dashboard wording should reflect that.
3. **Ingestion mode:** incremental by date window with a short lookback to catch revisions (proposed, to be justified in the midterm).