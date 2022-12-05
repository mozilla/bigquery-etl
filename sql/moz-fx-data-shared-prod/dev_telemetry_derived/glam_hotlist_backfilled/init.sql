CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.dev_telemetry_derived.glam_hotlist_backfilled` ( app_version INT64,
    os STRING,
    app_build_id STRING,
    process STRING,
    metric STRING,
    channel STRING,
    key STRING,
    client_agg_type STRING,
    metric_type STRING,
    total_users INT64,
    histogram STRING,
    percentiles STRING,
    backfill_date DATE );