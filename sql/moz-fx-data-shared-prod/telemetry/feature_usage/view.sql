CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.feature_usage`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.feature_usage_v2`
