-- Cumulative all-time Firefox first_run installs per referral (invite) code.
-- Full refresh over referral_installs_daily_v1; see metadata.yaml.
SELECT
  invite_code,
  SUM(install_count) AS total_installs,
FROM
  `moz-fx-data-shared-prod.firefox_referral_derived.referral_installs_daily_v1`
WHERE
  -- daily_v1 sets require_partition_filter; this all-time filter scans every
  -- partition while satisfying that requirement (the program produces no data
  -- before 2026).
  submission_date > DATE '2026-01-01'
GROUP BY
  invite_code
HAVING
  total_installs > 0
