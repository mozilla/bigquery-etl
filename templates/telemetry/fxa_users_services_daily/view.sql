CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fxa_users_services_daily` AS
SELECT
  * REPLACE(cn.code AS country),
  ls.country AS country_name
FROM
  `moz-fx-data-shared-prod.telemetry_derived.fxa_users_services_daily_v1` ls
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_names_v1` cn
ON
  cn.name = ls.country
