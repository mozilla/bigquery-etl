CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_users_services_devices_daily`
AS
SELECT
  * REPLACE (cn.code AS country),
  fxa.country AS country_name
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_services_devices_daily_v1` AS fxa
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_names_v1` AS cn
  ON cn.name = fxa.country
