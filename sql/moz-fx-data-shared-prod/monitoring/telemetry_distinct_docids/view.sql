CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.telemetry_distinct_docids`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.telemetry_distinct_docids_v1`
