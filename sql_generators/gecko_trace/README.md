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

## Pipeline

For each Firefox application, the generator creates five derived tables.

### Dimensions (`script.sql`, MERGE on the signature)

1. **gecko_trace_events_v1** — One row per `event_signature`.
   The signature is a SHA256 hash of (source_file, source_line, result).
   A new signature gets a new `stable_event_id`. Rows keep `first_seen_date` and `last_seen_date`.

2. **gecko_trace_traces_v1** — One row per `trace_signature`.
   The signature is a SHA256 hash of the event signatures in root-to-leaf order.
   The `trace_key` is a SHA256 hash of the `stable_event_id`s in the same order.
   A new signature with a known `trace_key` reuses the existing `stable_trace_id`.
   This table reads `gecko_trace_events_v1`, so it runs after it.

### Bridge (`script.sql`, insert only)

3. **gecko_trace_trace_events_v1** — One row per (`trace_signature`, `event_position`).
   Rows are written once, when a trace signature is first seen.

### Daily facts (`query.sql`, partitioned by `submission_date`)

4. **gecko_trace_events_daily_v1** — One row per (`submission_date`, `event_signature`) with `hit_count`.
5. **gecko_trace_traces_daily_v1** — One row per (`submission_date`, `trace_signature`) with `hit_count` and `avg_duration_nano`.

A re-run of the same day replaces the partition.

### Aggregate views

The `gecko_trace_aggregates` dataset has these views. Each one combines all applications.

- **events** — The events dimension with `app_id`.
- **traces** — The traces dimension with `app_id`. Filter on `first_seen_date` to find new trace patterns.
- **trace_events** — The bridge joined to both dimensions. Gives the ordered stable ids and source locations of each trace pattern.
- **events_daily** — Daily event counts with `stable_event_id`.
- **traces_daily** — Daily trace counts with `stable_trace_id`.

## Applications

The generator creates tables for these Firefox applications:

- `firefox_desktop`
- `org_mozilla_fenix_nightly`
- `org_mozilla_firefox_beta`

To add an application, add it to the `APPLICATIONS` list in `__init__.py`.

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
      event_ctes.sql              # Span extraction and event hashing
      trace_chain_ctes.sql        # Span tree walk and trace signatures
    gecko_trace_events_v1/        script.sql, metadata.yaml, schema.yaml
    gecko_trace_traces_v1/        script.sql, metadata.yaml, schema.yaml
    gecko_trace_trace_events_v1/  script.sql, metadata.yaml, schema.yaml
    gecko_trace_events_daily_v1/  query.sql, metadata.yaml, schema.yaml
    gecko_trace_traces_daily_v1/  query.sql, metadata.yaml, schema.yaml
  aggregates/
    dataset_metadata.yaml
    events/        view.sql, metadata.yaml
    traces/        view.sql, metadata.yaml
    trace_events/  view.sql, metadata.yaml
    events_daily/  view.sql, metadata.yaml
    traces_daily/  view.sql, metadata.yaml
```
