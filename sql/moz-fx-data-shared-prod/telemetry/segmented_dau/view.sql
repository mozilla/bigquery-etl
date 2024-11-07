CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.segmented_dau`
AS
SELECT
  submission_date,
  country,
  app_name,
  adjust_network,
  attribution_medium,
  attribution_source,
  first_seen_year,
  channel,
  install_source,
  is_default_browser,
  os_grouped,
  segment,
  SUM(dau) AS dau
FROM
  `moz-fx-data-shared-prod.telemetry.active_users_aggregates` AS active_users_aggregates
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_codes_v1` AS countries
  ON active_users_aggregates.country = countries.code
WHERE
  submission_date >= DATE('2022-01-01')
  AND LOWER(app_name) NOT LIKE "%browserstack%"
GROUP BY
  submission_date,
  country,
  app_name,
  adjust_network,
  attribution_medium,
  attribution_source,
  first_seen_year,
  channel,
  install_source,
  is_default_browser,
  os_grouped,
  segment
