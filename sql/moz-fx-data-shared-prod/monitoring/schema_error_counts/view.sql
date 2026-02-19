CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.schema_error_counts`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.schema_error_counts_v2`
