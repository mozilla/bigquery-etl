CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_derived.suggest_impression_rate_v1`(
    submission_minute TIESTAMP,
    n INT64
  )
PARTITION BY
  submission_minute
