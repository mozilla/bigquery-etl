CREATE TABLE IF NOT EXISTS
  `moz-fx-glam-prod.glam_etl.sampled_metrics_v1`(
    timestamp TIMESTAMP,
    metric_type STRING,
    metric_name STRING,
    sample_rate FLOAT64
  )
