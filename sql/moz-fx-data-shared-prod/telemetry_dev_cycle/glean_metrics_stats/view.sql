CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_dev_cycle.glean_metrics_stats`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_dev_cycle_derived.glean_metrics_stats_v1`
