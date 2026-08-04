# Gecko Trace SQL Generator

This generator creates BigQuery tables and views for Gecko trace data from Firefox applications.

## Pipeline

The pipeline has three derived tables and three aggregate views.

### Derived tables

For each Firefox application, the generator creates these tables:

1. **gecko_trace_events_v1** — One row per (event_signature, submission_date).
   The event signature is a SHA256 hash of (source_file, source_line, result).
   Each row has a stable UUID that does not change after the first insert.

2. **gecko_trace_traces_v1** — One row per (trace_signature, submission_date).
   The trace signature is a SHA256 hash of all event signatures in root-to-leaf order.
   The generator uses a recursive CTE to walk the span tree.
   Each row has a stable UUID that does not change after the first insert.

3. **gecko_trace_trace_events_v1** — One row per (stable_trace_id, stable_event_id).
   This table links traces to their events.

All three tables use MERGE scripts. A re-run of the same day overwrites the daily counts. It does not add to them.

### Aggregate views

The `gecko_trace_aggregates` dataset has these views:

- **events** — Sums daily hit counts across all dates and applications. Shows first and latest seen dates.
- **traces** — Sums daily hit counts across all dates and applications. Shows first and latest seen dates and weighted average duration.
- **trace_events** — Sums daily hit counts for each trace-event link.

## Applications

The generator creates tables for these Firefox applications:

- `firefox_desktop`
- `org_mozilla_fenix_nightly`
- `org_mozilla_firefox_beta`

To add an application, add it to the `APPLICATIONS` list in `__init__.py`.

## Usage

```bash
./bqetl generate gecko_trace
```

Use `--output-dir` to set the output directory. Use `--target-project` to set the BigQuery project.

## File structure

```
templates/
  derived/
    _shared/
      trace_chain_ctes.sql        # Shared CTEs for span tree walk
    gecko_trace_events_v1/
      script.sql, metadata.yaml, schema.yaml, backfill.yaml
    gecko_trace_traces_v1/
      script.sql, metadata.yaml, schema.yaml, backfill.yaml
    gecko_trace_trace_events_v1/
      script.sql, metadata.yaml, schema.yaml, backfill.yaml
  aggregates/
    dataset_metadata.yaml
    events/      view.sql, metadata.yaml
    traces/      view.sql, metadata.yaml
    trace_events/  view.sql, metadata.yaml
```
