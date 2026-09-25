# Gecko Trace SQL Generator

This generator creates BigQuery tables and views for Gecko trace data from Firefox applications.

## Model

Signatures are hashes of source code coordinates. They are stable within a Firefox release
and change when the source moves. Stable ids are UUIDs that identify the same event or trace
across releases. A mapper (outside this generator) can point a new signature at an existing
stable id.

Tables that count things are keyed by signature and partitioned by day. They are never
rewritten when the mapper runs. Tables that map signatures to stable ids are small and
are the only tables the mapper changes. Views join the two so that consumers see stable ids.

The mapper is the daily `event_mapper_v1` job described below. It uses Searchfox blame data
to find the same source line in two Firefox releases.

## Pipeline

For each Firefox application, the generator creates nine derived tables.

### Dimensions (`script.sql`, MERGE on the signature, DAG `bqetl_gecko_trace`)

1. **gecko_trace_events_v1** — One row per `event_signature`.
   The signature is a SHA256 hash of (source_file, source_line, result).
   A new signature gets a new `stable_event_id`. Rows keep `first_seen_date` and `last_seen_date`.

2. **gecko_trace_traces_v1** — One row per `trace_signature`.
   The signature is a SHA256 hash of the event signatures in root-to-leaf order.
   The `trace_key` is a SHA256 hash of the `stable_event_id`s in the same order.
   A new signature with a known `trace_key` reuses the existing `stable_trace_id`.
   This table reads `gecko_trace_events_v1`, so it runs after it.

### Bridge (`script.sql`, insert only, DAG `bqetl_gecko_trace`)

3. **gecko_trace_trace_events_v1** — One row per (`trace_signature`, `event_position`).
   Rows are written once, when a trace signature is first seen.

### Daily facts (`query.sql`, partitioned by `submission_date`, DAG `bqetl_gecko_trace`)

4. **gecko_trace_events_daily_v1** — One row per (`submission_date`, `event_signature`) with `hit_count`.
5. **gecko_trace_traces_daily_v1** — One row per (`submission_date`, `trace_signature`) with `hit_count` and `avg_duration_nano`.
6. **gecko_trace_platform_counts_v1** — One row per (`submission_date`, `trace_signature`, build, OS, OS version, architecture) with `hit_count`.

A re-run of the same day replaces the partition.

### Reporter state (`script.sql`, create only, DAG `bqetl_gecko_trace_weekly`)

7. **gecko_trace_bug_reports_v1** — One row per `stable_trace_id` that has a Bugzilla bug.
   The script only creates the table. The weekly reporter inserts and updates the rows.

### Mapper caches (`script.sql`, create only, DAG `bqetl_gecko_trace`)

8. **gecko_trace_source_revisions_v1** — One row per `app_build`. Gives the Searchfox tree,
   the hg revision and the git revision of the build. A NULL `git_rev` marks a build that
   could not be resolved.
9. **gecko_trace_blame_lines_v1** — One row per event source line at one git revision.
   Gives the revision that introduced the line and the position the line had there.
   A line that does not exist at a revision has NULL origin fields.

The scripts only create the tables. The daily mapper inserts the rows.

### Aggregate views

The `gecko_trace_aggregates` dataset has these views. Each one combines all applications.

- **events** — The events dimension with `app_id`.
- **traces** — The traces dimension with `app_id`. Filter on `first_seen_date` to find new trace patterns.
- **trace_events** — The bridge joined to both dimensions. Gives the ordered stable ids and source locations of each trace pattern.
- **events_daily** — Daily event counts with `stable_event_id`.
- **traces_daily** — Daily trace counts with `stable_trace_id`.
- **platform_counts** — Daily trace counts per platform with `stable_trace_id`.
- **bug_reports** — Bugzilla bugs filed per `stable_trace_id`.
- **source_revisions** — Build ids with their Searchfox tree and revisions.
- **blame_lines** — Cached Searchfox blame data for event source lines.

## Daily event mapper

`sql/moz-fx-data-shared-prod/gecko_trace_aggregates/event_mapper_v1/query.py` runs at the end
of the `bqetl_gecko_trace` DAG. It is hand-written, not generated. The logic is in
`bigquery_etl/gecko_trace/event_mapper.py` and the Searchfox client in
`bigquery_etl/gecko_trace/searchfox.py`. For each application it:

1. Finds events whose (build, source file, source line) has no cached blame. On the first run
   that is every event; later it is only the new events of the day. It looks unknown builds
   up in `telemetry.buildhub2` to get the hg revision and repository, picks the Searchfox tree
   for the repository, and asks the Searchfox `hgrev` endpoint for the git revision.
2. Fetches the Searchfox `blame-lines` endpoint once per revision for all lines that need it,
   and stores one row per event line in `gecko_trace_blame_lines_v1`. A revision that has no blame yet,
   for example a nightly from the same day, is not cached and is tried again on the next run.
3. Gives events that share the blame triple (introducing revision, path, line) and the result the
   `stable_event_id` of the oldest such event.
4. Recomputes `trace_key` from the ordered stable event ids and gives traces with the same key the
   `stable_trace_id` of the oldest trace. A trace whose events do not all map keeps its own id.

Searchfox stores blame only for lines that were not modified. A line touched by a reformatting
commit gets a new identity and the event counts as new, which is the same result as without the
mapper. Builds of Android applications are not in buildhub2 and are not resolved.

## Weekly Bugzilla reporter

`sql/moz-fx-data-shared-prod/gecko_trace_aggregates/new_trace_reporter_v1/query.py` runs in the
`bqetl_gecko_trace_weekly` DAG. It is hand-written, not generated. The logic is in
`bigquery_etl/gecko_trace/bugzilla_reporter.py`. For each application it:

1. Files one bug for each `stable_trace_id` first seen in the last 7 days that has no bug yet,
   and records the bug in `gecko_trace_bug_reports_v1`.
2. Posts one comment with fresh counts, platforms and source locations to the bug of each known
   trace pattern that was seen in the last 7 days. It never changes the bug status.

The reporter reads the `BUGZILLA_API_KEY` environment variable. In Airflow the key comes from the
`bqetl_gecko_trace__bugzilla_api_key` secret declared in the task metadata. The secret must exist
before the first production run.

## Applications

The generator creates tables for these Firefox applications:

- `firefox_desktop`
- `org_mozilla_fenix_nightly`
- `org_mozilla_firefox_beta`

To add an application, add it to the `APPLICATIONS` list in `__init__.py` and in
`bigquery_etl/gecko_trace/__init__.py`.

## Usage

```bash
./bqetl generate gecko_trace --output-dir=/tmp/sql_test
```

Do not commit the generated files under `sql/`. CI generates them.
Use `--target-project` to set the BigQuery project.

## File structure

```
templates/
  derived/
    _shared/
      event_ctes.sql                  # Span extraction and event hashing
      trace_chain_ctes.sql            # Span tree walk and trace signatures
    gecko_trace_events_v1/            script.sql, metadata.yaml, schema.yaml
    gecko_trace_traces_v1/            script.sql, metadata.yaml, schema.yaml
    gecko_trace_trace_events_v1/      script.sql, metadata.yaml, schema.yaml
    gecko_trace_events_daily_v1/      query.sql, metadata.yaml, schema.yaml
    gecko_trace_traces_daily_v1/      query.sql, metadata.yaml, schema.yaml
    gecko_trace_platform_counts_v1/   query.sql, metadata.yaml, schema.yaml
    gecko_trace_bug_reports_v1/       script.sql, metadata.yaml, schema.yaml
    gecko_trace_source_revisions_v1/  script.sql, metadata.yaml, schema.yaml
    gecko_trace_blame_lines_v1/       script.sql, metadata.yaml, schema.yaml
  aggregates/
    dataset_metadata.yaml
    events/           view.sql, metadata.yaml
    traces/           view.sql, metadata.yaml
    trace_events/     view.sql, metadata.yaml
    events_daily/     view.sql, metadata.yaml
    traces_daily/     view.sql, metadata.yaml
    platform_counts/  view.sql, metadata.yaml
    bug_reports/      view.sql, metadata.yaml
    source_revisions/ view.sql, metadata.yaml
    blame_lines/      view.sql, metadata.yaml
```
