CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_dev_cycle.experiments_stats`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_dev_cycle_external.experiments_stats_v1`
