# Plausible raw exports (firefox.com)

Plausible Analytics is the EU-hosted web analytics vendor used for firefox.com
(`site_id` 86997652). It delivers two parquet files per day into the bucket
provisioned in [DENG-11379][deng-11379]:

```
gs://moz-fx-data-prod-external-plausible-data/firefox-com/<YYYY-MM-DD>/events.parquet
gs://moz-fx-data-prod-external-plausible-data/firefox-com/<YYYY-MM-DD>/sessions.parquet
```

`events_v1` and `sessions_v1` are faithful landings of those files, one file to
one day partition. The shared mechanics live in [`load.py`](load.py); each
table's `query.py` supplies only its typed `SELECT`.

## How the vendor windows each file

Measured across the `2026-07-30` … `2026-08-05` exports:

| | `events.parquet` | `sessions.parquet` |
|---|---|---|
| One row per | event | session |
| Filed by | event `timestamp` | session `start` |
| Values outside the export date | none | none |
| Duplicate keys within a file | n/a | none (`session_id` unique) |
| Same key in two files | none | none |

Both files are cleanly windowed on their filing column, which is why each one
maps onto exactly one partition. `load.py` asserts this on every run rather
than trusting it: if a file ever spans two dates, the load fails instead of
silently dropping the rows that fall outside the partition it replaces.

## The midnight window-split

The two files are windowed on *different* columns, and that is where sessions
spanning midnight cause trouble.

A session's `sessions_v1` row is filed by `start`, but its counters
(`pageviews`, `event_count`, `duration`, `exit_page`) describe the **whole
visit**. Its `timestamp` (last update) can therefore land on the following day
— observed as late as 09:13 the next morning. Its events, meanwhile, are filed
by their own timestamps, so they are **split across two `events_v1`
partitions**.

Measured on the `2026-08-05` export: 1,445 events (0.07%) in the `2026-08-05`
events file belong to 66 sessions that started on `2026-08-04`. A further 78
`session_id`s across the two days belong to sessions that started earlier still.

Two concrete consequences:

**1. Session counters are not day-aligned with event rows.** For a single date
`D`:

```sql
-- These do NOT match, and are not supposed to.
SELECT SUM(pageviews) FROM sessions_v1 WHERE DATE(`start`) = D;      -- whole visits
SELECT COUNT(*) FROM events_v1
  WHERE DATE(`timestamp`) = D AND name = 'pageview';                 -- truncated at midnight
```

Use `sessions_v1` counters when you want per-visit totals, and `events_v1` when
you want per-day activity. Do not reconcile one against the other.

**2. Joining events to sessions must span two days.** Restricting both sides to
the same date silently drops the post-midnight tail:

```sql
-- WRONG: loses events belonging to sessions that started the previous day.
SELECT ...
FROM events_v1 AS e
JOIN sessions_v1 AS s USING (session_id)
WHERE DATE(e.`timestamp`) = @date AND DATE(s.`start`) = @date

-- RIGHT: widen the sessions side to cover sessions that started earlier.
SELECT ...
FROM events_v1 AS e
JOIN sessions_v1 AS s USING (session_id)
WHERE
  DATE(e.`timestamp`) = @date
  AND DATE(s.`start`) BETWEEN DATE_SUB(@date, INTERVAL 1 DAY) AND @date
```

One day of lookback covered every carry-over observed in the sample. Widen it
if you need to be exact about the long tail of multi-day sessions, which is why
`require_partition_filter` is deliberately left off on both tables.

Conversely, to assemble a complete session that started on `@date`, read
`events_v1` for `@date` **and** `@date + 1`.

## Notes on the data

- **`session_id` is `NUMERIC`, not `INT64`.** Plausible emits it as an unsigned
  64-bit hash; the observed maximum, 18446721902263609798, exceeds the INT64
  range. Casting it to `INT64` anywhere downstream will overflow.

  This also means **BigQuery cannot load these files as delivered**. It reads a
  parquet `UINT_64` column into a signed `INT64` and fails the entire load once
  a value exceeds the `INT64` maximum, and no load-job setting retargets it —
  only DECIMAL logical types can be redirected to `NUMERIC`. So `load.py`
  downloads each file and rewrites its unsigned 64-bit columns (`site_id` and
  `session_id`) as strings before uploading, streaming row group by row group to
  bound memory; the typed `SELECT` then casts them to `INT64` and `NUMERIC`. If
  Plausible ever changes the export to emit `session_id` as a string or a
  decimal, that rewrite becomes a no-op and can be dropped.
- **`name = 'engagement'` is synthetic.** Plausible emits it during page
  interaction and it is ~77% of event rows. Filter to `name = 'pageview'` for
  pageview-only analysis. `sessions_v1.event_count` already excludes it.
- **Empty strings are normalized to `NULL` on load.** The source parquet
  declares its string columns non-nullable, so the vendor encodes "not
  collected" as `""` (72% of `subdivision2_code`, 79% of `referrer`).
- **`props` / `entry_props` stay raw JSON strings**, so a malformed or
  newly-shaped vendor payload cannot fail the daily load. Use
  `JSON_VALUE(props, '$.method')` downstream. Observed keys: `method`,
  `platform`, `product`, `release_channel`, `download_language`,
  `gtm.uniqueEventId`.
- **`scroll_depth` and `engagement_time` are only meaningful on engagement
  events.** Other event types carry `0`, as delivered. `engagement_time` is not
  sanity-bounded by the vendor (observed maxima exceed a day), so cap outliers
  before averaging.
- **The `revenue_*` columns are always `NULL`** — firefox.com has no Plausible
  revenue goals configured. They are retained to stay faithful to the export.

## Export cadence

The `bqetl_plausible` DAG runs at 12:00 UTC for the previous day (`{{ds}}`).
That timing is provisional: at the time of writing, all seven available exports
(`2026-07-30` … `2026-08-05`) were written in a single batch on `2026-08-06`
between 09:13 and 09:22 UTC, and no file has landed since. A steady-state
delivery time has therefore never been observed. Confirm the vendor's actual
schedule against [DENG-11379][deng-11379] and adjust `schedule_interval` before
relying on the DAG.

A missing file fails the task with an explicit message and leaves the
destination partition untouched, so a late export is fixed by clearing the task
once the file lands.

The DAG carries `triage/no_triage` because it is expected to fail daily until
the vendor feed resumes. Drop that tag once the cadence is confirmed.

## Backfilling

Both `query.py` files expose a module-level `main()` and accept a fully
qualified `--destination-table`, which is what managed backfills need from a
query script (supported since #8940). A `backfill.yaml` entry looks like:

```yaml
query_script_entrypoint: main
query_script_date_arg: date
query_script_args:
- --destination-table=moz-fx-data-shared-prod.backfills_staging_derived.plausible_external__events_v1_2026_08_20
```

`bqetl backfill create` writes that `--destination-table` line for you; the
staging table name has to match for the copy to production to work.

Each date fills its own partition of the staging table, so a backfill is as
rerunnable as a scheduled run. The same flag is the way to load into a scratch
dataset when testing:

```
python sql/moz-fx-data-shared-prod/plausible_external/events_v1/query.py \
  --date 2026-08-05 \
  --destination-table mozdata.analysis.plausible_events_v1
```

Either way the destination table must already exist and be day-partitioned on
`timestamp` (events) or `start` (sessions) — this load writes a partition
decorator, which cannot create a table.

To run it directly rather than through a `backfill.yaml`:

```
./bqetl query backfill plausible_external.events_v1 \
  --start-date 2026-07-30 --end-date 2026-08-05 \
  --query-script-entrypoint main --query-script-date-arg date
```

[deng-11379]: https://mozilla-hub.atlassian.net/browse/DENG-11379
