CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_users_services_daily`
AS
-- Manually specifying fields to keep this "interface"
-- compatible with any downstream dependencies
SELECT
  submission_date,
  user_id,
  `service`,
  cn.code AS country,
  fxa.country AS country_name,
  `language`,
  app_version,
  os_name,
  os_version,
  seen_in_tier1_country,
  registered,
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_services_daily_v1` AS fxa
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_names_v1` AS cn
ON
  cn.name = fxa.country
