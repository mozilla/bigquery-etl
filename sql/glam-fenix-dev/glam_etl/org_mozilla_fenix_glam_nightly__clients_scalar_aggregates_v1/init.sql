-- init for org_mozilla_fenix_glam_nightly__clients_scalar_aggregates_v1;
CREATE TABLE IF NOT EXISTS
  `glam-fenix-dev.glam_etl.org_mozilla_fenix_glam_nightly__clients_scalar_aggregates_v1`(
    client_id STRING,
    ping_type STRING,
    os STRING,
    app_version INT64,
    app_build_id STRING,
    channel STRING,
    scalar_aggregates ARRAY<
      STRUCT<metric STRING, metric_type STRING, key STRING, agg_type STRING, value FLOAT64>
    >
  )
PARTITION BY
  RANGE_BUCKET(app_version, GENERATE_ARRAY(0, 100, 1))
CLUSTER BY
  app_version,
  channel,
  client_id
