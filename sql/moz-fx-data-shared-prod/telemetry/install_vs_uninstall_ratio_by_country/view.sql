CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.install_vs_uninstall_ratio_by_country`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.install_vs_uninstall_ratio_by_country_v1`
