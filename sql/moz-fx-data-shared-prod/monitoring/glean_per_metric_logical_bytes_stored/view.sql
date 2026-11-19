CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.glean_per_metric_logical_bytes_stored`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.glean_per_metric_logical_bytes_stored_v1`
