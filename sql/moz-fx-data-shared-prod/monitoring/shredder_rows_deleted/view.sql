CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.shredder_rows_deleted`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.shredder_rows_deleted_v1`
