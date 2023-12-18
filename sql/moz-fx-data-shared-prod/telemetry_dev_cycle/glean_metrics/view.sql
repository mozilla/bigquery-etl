CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_dev_cycle.glean_metrics`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_dev_cycle_derived.glean_metrics_v1`
