CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fx_health_ind_bookmarks_by_os_version`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.fx_health_ind_bookmarks_by_os_version_v1`
