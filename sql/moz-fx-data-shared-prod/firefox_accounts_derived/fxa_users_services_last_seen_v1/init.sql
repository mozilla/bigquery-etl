CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_services_last_seen_v1`
PARTITION BY
  submission_date
CLUSTER BY
  service,
  user_id
AS
SELECT
  CAST(NULL AS DATE) AS submission_date,
  0 AS days_seen_bits,
  0 AS days_seen_in_tier1_country_bits,
  0 AS days_registered_bits,
  FALSE AS resurrected_same_service,
  FALSE AS resurrected_any_service,
  -- We make sure to delay * until the end so that as new columns are added
  -- to fxa_users_services_daily, we can add those columns in the same order to the end
  -- of this schema, which may be necessary for the daily join query between
  -- the two tables to validate.
  * EXCEPT (submission_date, seen_in_tier1_country, registered)
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_services_daily_v1`
WHERE
  -- Output empty table and read no input rows
  FALSE
