-- Cumulative all-time Firefox first_run installs per referral (invite) code.
--
-- Full refresh over referral_installs_daily_v1. Each client is first-seen
-- exactly once (first_seen_date = submission_date in the daily query), so a
-- straight SUM across daily partitions gives the all-time distinct install
-- count per code without double-counting.
--
-- Zero-install codes are omitted (HAVING) — the Website team only ingests codes
-- with at least one attributed install. This table is the source for the
-- CSV-to-GCS extract (referral_installs_totals_to_gcs_v1).
SELECT
  invite_code,
  SUM(install_count) AS total_installs,
FROM
  `moz-fx-data-shared-prod.firefox_referral_derived.referral_installs_daily_v1`
WHERE
  -- daily_v1 sets require_partition_filter; this all-time filter scans every
  -- partition while satisfying that requirement.
  submission_date > DATE '2020-01-01'
GROUP BY
  invite_code
HAVING
  total_installs > 0
