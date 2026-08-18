-- Cumulative all-time Firefox first_run installs per referral (invite) code.
--
-- Full refresh over referral_installs_daily_v1. The Glean `referrals` ping fires
-- once per profile, so summing install_count across daily partitions gives the
-- all-time distinct install count per code. Rows are summed across channels and
-- platforms: daily_v1 is grained by normalized_channel, and this table is not.
--
-- CHANNEL: pre-release traffic is included, because as of 2026-08-18 nightly is
-- the only channel reporting the ping. Once 155 reaches release, decide whether
-- the CSV that firefox.com ingests should exclude non-release installs — this is
-- the right place to filter, so the daily table stays complete.
--
-- Zero-install codes are omitted (HAVING) — the Website team only ingests codes
-- with at least one attributed install.
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
