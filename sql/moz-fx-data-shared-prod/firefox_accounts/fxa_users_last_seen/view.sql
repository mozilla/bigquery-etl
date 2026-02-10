CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_users_last_seen`
AS
SELECT
  mozfun.bits28.days_since_seen(days_seen_bits) AS days_since_seen,
  mozfun.bits28.days_since_seen(
    days_seen_in_tier1_country_bits
  ) AS days_since_seen_in_tier1_country,
  mozfun.bits28.days_since_seen(days_registered_bits) AS days_since_registered,
  mozfun.bits28.days_since_seen(days_seen_no_monitor_bits) AS days_since_seen_no_monitor,
  *
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_last_seen_v1`
