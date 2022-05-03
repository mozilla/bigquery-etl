CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.unified_metrics`
AS
SELECT
  * REPLACE (COALESCE(country, '??') AS country)
FROM
  `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
