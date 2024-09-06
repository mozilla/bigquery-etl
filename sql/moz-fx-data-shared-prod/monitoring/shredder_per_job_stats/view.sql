CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.shredder_per_job_stats`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.shredder_per_job_stats_v1`
