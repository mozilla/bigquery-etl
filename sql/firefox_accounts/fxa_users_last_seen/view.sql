CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_users_last_seen` AS
SELECT
  -- We cannot use UDFs in a view, so we paste the body of udf.bitpos(bits) literally here.
  CAST(SAFE.LOG(days_seen_bits & -days_seen_bits, 2) AS INT64) AS days_since_seen,
  CAST(SAFE.LOG(days_seen_in_tier1_country_bits & -days_seen_in_tier1_country_bits, 2) AS INT64) AS days_since_seen_in_tier1_country,
  CAST(SAFE.LOG(days_registered_bits & -days_registered_bits, 2) AS INT64) AS days_since_registered,
  CAST(SAFE.LOG(days_seen_no_monitor_bits & -days_seen_no_monitor_bits, 2) AS INT64) AS days_since_seen_no_monitor,
  *
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_last_seen_v1`
