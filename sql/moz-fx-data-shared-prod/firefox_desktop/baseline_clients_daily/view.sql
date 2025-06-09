-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_daily`
AS
SELECT
  *,
  mozfun.norm.glean_windows_version_info(
    normalized_os,
    normalized_os_version,
    windows_build_number
  ) AS windows_version
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_daily_v1`
