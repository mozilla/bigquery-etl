-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_first_seen`
AS
SELECT
  *,
  IF(
    LOWER(IFNULL(isp, '')) <> "browserstack"
    AND LOWER(IFNULL(COALESCE(distribution_id, distribution.name), '')) <> "mozillaonline",
    TRUE,
    FALSE
  ) AS is_desktop,
  mozfun.norm.glean_windows_version_info(
    normalized_os,
    normalized_os_version,
    windows_build_number
  ) AS windows_version
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_daily_v1`
WHERE
  is_new_profile
