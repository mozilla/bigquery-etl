CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.clients_histogram_aggregates_v1` (
    client_id STRING,
    os STRING,
    app_version INT64,
    app_build_id STRING,
    channel STRING,
    histogram_aggregates ARRAY <STRUCT<
      first_bucket INT64,
      last_bucket INT64,
      num_buckets INT64,
      latest_version INT64,
      metric STRING,
      metric_type STRING,
      key STRING,
      agg_type STRING,
      aggregates STRUCT<key_val ARRAY<STRUCT<key STRING, value INT64>>>
    >>
)
PARTITION BY RANGE_BUCKET(app_version, GENERATE_ARRAY(30, 200, 1))
CLUSTER BY app_version, channel, client_id
