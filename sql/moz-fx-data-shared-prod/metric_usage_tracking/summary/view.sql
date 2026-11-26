CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.metric_usage_tracking.summary`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.summary_v1`
