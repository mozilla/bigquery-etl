CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.firefox_desktop_usage_2021`
AS
SELECT
  usage.* REPLACE (
    COALESCE(cdou, 0) AS cdou,
    COALESCE(mau, 0) AS mau,
    COALESCE(wau, 0) AS wau,
    COALESCE(dau, 0) AS dau,
    COALESCE(new_profiles, 0) AS new_profiles,
    COALESCE(cumulative_new_profiles, 0) AS cumulative_new_profiles
  ),
  (source IS NOT NULL OR campaign IS NOT NULL) AS attributed,
  cc.name AS country_name,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.firefox_desktop_usage_v1` AS usage
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_codes_v1` AS cc
ON
  (usage.country = cc.code)
