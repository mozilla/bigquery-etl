CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.clients_scalar_aggregates_v1` (
    client_id STRING,
    os STRING,
    app_version INT64,
    app_build_id STRING,
    channel STRING,
    scalar_aggregates ARRAY <STRUCT<metric STRING,
    metric_type STRING,
    key STRING,
    process STRING,
    agg_type STRING,
    value FLOAT64>>)
PARTITION BY RANGE_BUCKET(app_version, GENERATE_ARRAY(30, 200, 1))
CLUSTER BY app_version, channel, client_id
