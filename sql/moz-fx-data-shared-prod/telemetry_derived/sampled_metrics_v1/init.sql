CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.sampled_metrics_v1`(
    start_date DATE,
    experimenter_slug STRING,
    is_rollout BOOL,
    app_name STRING,
    channel STRING,
    start_version INT64,
    end_date DATE,
    metric_type STRING,
    metric_name STRING,
    sample_rate FLOAT64
  )
