CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.shredder_progress`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.shredder_progress_v1`
