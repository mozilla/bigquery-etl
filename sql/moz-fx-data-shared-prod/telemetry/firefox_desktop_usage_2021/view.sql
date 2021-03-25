CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.firefox_desktop_usage_2021`
AS
SELECT
  * REPLACE (
    COALESCE(cdou, 0) AS cdou,
    COALESCE(mau, 0) AS mau,
    COALESCE(wau, 0) AS wau,
    COALESCE(dau, 0) AS dau,
    COALESCE(new_profiles, 0) AS new_profiles,
    COALESCE(cumulative_new_profiles, 0) AS cumulative_new_profiles
  ),
FROM
  `moz-fx-data-shared-prod.telemetry_derived.firefox_desktop_usage_v1`
