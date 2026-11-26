CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.metric_usage_tracking.metrics`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.metrics_v1`
