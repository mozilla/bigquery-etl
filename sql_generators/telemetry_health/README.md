# Telemetry Health

Generates queries for the Glean Health Scorecard, which monitors telemetry quality across Firefox applications.

Each query combines data from Firefox Desktop, Firefox Android (Fenix), and Firefox iOS into a single table with an `application` column for filtering. All queries use a 1% sample (`sample_id = 0`).

## Generated Queries

- `glean_errors_v1`: Counts Glean metrics (from metrics pings) with recording errors exceeding 1% of sampled clients per day
- `ping_latency_v1`: Reports latency percentiles (p95, median) for baseline pings: collection-to-submission, submission-to-ingestion, and collection-to-ingestion
- `ping_volume_p80_v1`: 80th percentile of baseline ping count per client per day
- `sequence_holes_v1`: Percentage of sampled clients with sequence number gaps in baseline pings within a single day

## Running the Generator

```bash
./bqetl generate telemetry_health
```

## Configuration

Application list and query settings are defined in `templates/templating.yaml`.
