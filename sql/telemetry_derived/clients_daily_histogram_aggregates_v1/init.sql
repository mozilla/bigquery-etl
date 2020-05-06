CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.clients_daily_histogram_aggregates_v1` (
    submission_date DATE,
    sample_id INT64,
    client_id STRING,
    os STRING,
    app_version STRING,
    app_build_id STRING,
    channel STRING,
    histogram_aggregates ARRAY <STRUCT<metric STRING,
    metric_type STRING,
    key STRING,
    process STRING,
    agg_type STRING,
    bucket_range STRUCT<first_bucket INT64,
    last_bucket INT64,
    num_buckets INT64>,
    value ARRAY <STRUCT<key STRING,
    value INT64>>>>)
PARTITION BY submission_date
CLUSTER BY app_version, channel
OPTIONS(
    require_partition_filter=true,
    partition_expiration_days=7
)
