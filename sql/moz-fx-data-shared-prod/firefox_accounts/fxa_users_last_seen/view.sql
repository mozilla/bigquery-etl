CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_users_last_seen`
AS
SELECT
  mozfun.bits28.days_since_seen(days_seen_bits) AS days_since_seen,
  mozfun.bits28.days_since_seen(
    days_seen_in_tier1_country_bits
  ) AS days_since_seen_in_tier1_country,
  mozfun.bits28.days_since_seen(days_registered_bits) AS days_since_registered,
  mozfun.bits28.active_in_range(days_seen_bits, -6, 7) AS active_this_week,
  mozfun.bits28.active_in_range(days_seen_bits, -13, 7) AS active_last_week,
  DATE_DIFF(submission_date, first_run_date, DAY) BETWEEN 0 AND 6 AS new_this_week,
  DATE_DIFF(${submission_date}, first_run_date, DAY) BETWEEN 7 AND 13 AS new_last_week,
  -- TODO: remove the `days_since_seen_no_monitor` field once we confirm no downstream usage.
  CAST(NULL AS INTEGER) AS days_since_seen_no_monitor,
  *
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_last_seen_v2`
