# Newtab Topsite Tile Daily

## Overview
This table provides a daily, per‑tile summary of engagement with Firefox New Tab topsites.
Each row represents one topsite tile for a specific client and environment on a given `submission_date`.
Metrics include impressions, clicks, dismissals, pin/unpin counts, and whether any content engagement occurred.
Experiment enrollment data is also included to support experiment analysis.

## Process Summary
Data originates from `firefox_desktop_derived.newtab_visits_daily_v2`.
Key steps:
- Unnest `topsite_tile_components` to generate one row per tile per New Tab visit.
- Retain client, environment, UI configuration, and tile attributes (position, sponsorship status).
- Aggregate engagement metrics across visits.
- Count distinct visits where the tile appeared.
- Carry experiment context from the original visit data.

The resulting table is **partitioned by `submission_date`** and **clustered by `channel` and `country`** for efficient querying.

## Example Use Cases

### 1. Compare Click‑through Rate by Tile Position
```sql
SELECT
  position,
  AVG(click_count / NULLIF(impression_count, 0)) AS ctr
FROM newtab_topsite_tile_daily
WHERE submission_date = '2025-12-01'
GROUP BY position;
```

### 2. Sponsored vs. Organic Tile Performance
```sql
SELECT
  is_sponsored,
  SUM(impression_count) AS impressions,
  SUM(click_count) AS clicks
FROM newtab_topsite_tile_daily
WHERE submission_date BETWEEN '2025-12-01' AND '2025-12-10'
GROUP BY is_sponsored;
```

### 3. High‑Level Experiment Impact
```sql
SELECT
  e.key AS experiment,
  e.value.branch AS branch,
  SUM(click_count) AS clicks
FROM newtab_topsite_tile_daily t,
UNNEST(t.experiments) AS e
WHERE submission_date = '2025-12-01'
GROUP BY experiment, branch;
```
