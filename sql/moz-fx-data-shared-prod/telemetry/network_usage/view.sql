CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.network_usage`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.network_usage_v1`
