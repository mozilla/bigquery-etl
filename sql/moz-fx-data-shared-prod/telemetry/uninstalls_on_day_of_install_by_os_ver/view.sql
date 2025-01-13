CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.uninstalls_on_day_of_install_by_os_ver`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.uninstalls_on_day_of_install_by_os_ver_v1`
