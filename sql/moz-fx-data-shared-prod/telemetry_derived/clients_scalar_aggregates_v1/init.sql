CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.clients_scalar_aggregates_v1`(
    submission_date DATE,
    client_id STRING,
    os STRING,
    app_version INT64,
    app_build_id STRING,
    channel STRING,
    scalar_aggregates ARRAY<
      STRUCT<
        metric STRING,
        metric_type STRING,
        key STRING,
        process STRING,
        agg_type STRING,
        value FLOAT64
      >
    >
  )
PARTITION BY
  submission_date
CLUSTER BY
  app_version,
  channel,
  client_id
OPTIONS
  (require_partition_filter = TRUE, partition_expiration_days = 7)
