CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.core_clients_last_seen_v1`
AS
WITH with_days_since AS (
  SELECT
    mozfun.bits28.days_since_seen(days_seen_bits) AS days_since_seen,
    mozfun.bits28.days_since_seen(days_created_profile_bits) AS days_since_created_profile,
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.core_clients_last_seen_v1`
)
SELECT
  -- Include date_last_seen for compatibility with existing queries.
  DATE_SUB(submission_date, INTERVAL days_since_seen DAY) AS date_last_seen,
  *
FROM
  with_days_since
