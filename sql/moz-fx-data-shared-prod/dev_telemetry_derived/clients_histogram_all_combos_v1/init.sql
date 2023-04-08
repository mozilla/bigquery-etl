CREATE TABLE `moz-fx-data-shared-prod.dev_telemetry_derived.all_combos_test`
(
  channel STRING,
  sample_id INT64,
  client_id STRING,
  app_version INT64,
  first_bucket INT64,
  last_bucket INT64,
  num_buckets INT64,
  metric STRING,
  metric_type STRING,
  key STRING,
  process STRING,
  agg_type STRING,
  aggregates ARRAY<STRUCT<key STRING, value INT64>>,
  sampled BOOL,
  os STRING,
  app_build_id STRING
);
