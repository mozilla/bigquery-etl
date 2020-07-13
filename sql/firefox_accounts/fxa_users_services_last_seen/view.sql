CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_users_services_last_seen`
AS
SELECT
  mozfun.bits28.days_since_seen(days_seen_bits) AS days_since_seen,
  mozfun.bits28.days_since_seen(
    days_seen_in_tier1_country_bits
  ) AS days_since_seen_in_tier1_country,
  mozfun.bits28.days_since_seen(days_registered_bits) AS days_since_registered,
  fxa.* REPLACE (cn.code AS country),
  fxa.country AS country_name
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_services_last_seen_v1` AS fxa
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_names_v1` AS cn
ON
  cn.name = fxa.country
