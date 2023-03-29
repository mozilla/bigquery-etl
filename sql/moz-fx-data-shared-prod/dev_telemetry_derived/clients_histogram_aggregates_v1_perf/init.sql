CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.dev_telemetry_derived.clients_histogram_aggregates_v1_perf` (
    submission_date DATE,
    sample_id INT64,
    client_id STRING,
    os STRING,
    app_version INT64,
    app_build_id STRING,
    channel STRING,
    histogram_aggregates ARRAY <STRUCT<
      first_bucket INT64,
      last_bucket INT64,
      num_buckets INT64,
      metric STRING,
      metric_type STRING,
      key STRING,
      process STRING,
      agg_type STRING,
      aggregates ARRAY<STRUCT<key STRING, value INT64>>
    >>
)
PARTITION BY RANGE_BUCKET(sample_id, GENERATE_ARRAY(0, 99, 1))
CLUSTER BY app_build_id, channel
OPTIONS(
  require_partition_filter=true
);
