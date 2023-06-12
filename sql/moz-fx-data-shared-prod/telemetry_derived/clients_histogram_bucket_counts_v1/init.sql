CREATE TABLE `moz-fx-data-shared-prod.telemetry_derived.clients_histogram_bucket_counts_v1`
(
  os STRING,
  app_version INT64,
  app_build_id STRING,
  channel STRING,
  first_bucket INT64,
  last_bucket INT64,
  num_buckets INT64,
  metric STRING,
  metric_type STRING,
  key STRING,
  process STRING,
  agg_type STRING,
  record STRUCT<key STRING, value FLOAT64>
);
