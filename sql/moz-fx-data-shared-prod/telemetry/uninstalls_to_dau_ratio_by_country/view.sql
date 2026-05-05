CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.uninstalls_to_dau_ratio_by_country`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.uninstalls_to_dau_ratio_by_country_v1`
