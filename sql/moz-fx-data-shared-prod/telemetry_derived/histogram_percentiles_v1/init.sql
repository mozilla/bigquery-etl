CREATE TABLE `moz-fx-data-shared-prod.telemetry_derived.histogram_percentiles_v1`
(
  os STRING,
  app_version INT64,
  app_build_id STRING,
  channel STRING,
  metric STRING,
  metric_type STRING,
  key STRING,
  process STRING,
  first_bucket INT64,
  last_bucket INT64,
  num_buckets INT64,
  client_agg_type STRING,
  agg_type STRING,
  total_users INT64,
  aggregates ARRAY<STRUCT<KEY STRING, value FLOAT64>>,
  non_norm_aggregates ARRAY<STRUCT<KEY STRING, value FLOAT64>>,
)
CLUSTER BY metric, channel;
