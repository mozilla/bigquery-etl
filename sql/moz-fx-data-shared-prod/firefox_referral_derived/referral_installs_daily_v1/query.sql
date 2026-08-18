-- Daily count of Firefox first_run installs per referral (invite) code.
--
-- SOURCE: the dedicated Glean `referrals` ping, sent once on first run by a
-- profile that carries referral attribution data.
--   * Desktop: `firefox_desktop.referrals`, metric `browser.referral_code`
--     (bug 2055255, shipped in Firefox 155).
--   * Android: `fenix.referrals`, metric `referrals.code`
--     (bug 2062793, the Fenix equivalent).
--
-- WHY NOT ATTRIBUTION / GA4: Firefox deliberately strips the referral code out
-- of regular attribution data and submits it via this separate ping instead —
-- that is the entire point of bug 2055255. A referred install therefore lands in
-- `baseline_clients_first_seen_v1` with source=www.firefox.com, medium=referral,
-- campaign=firefox-referral and a valid dltoken, but `attribution.content` NULL.
-- The previous version of this query chased the code through GA4 `utm_content`
-- -> dl_token -> baseline_clients_first_seen and so returned zero rows every day
-- from launch; see DENG-11237. Do NOT reintroduce that path: the website does
-- send `content=fxrefer<code>` in the attribution code, but the client removes it
-- before it reaches any attribution field we can read.
--
-- CODE FORMAT: the ping carries the code BARE — `1WYYSNCNGE6S7WKGF`, not
-- `fxrefer1WYYSNCNGE6S7WKGF` — because the client strips the prefix when it
-- extracts the code. We strip an optional `fxrefer` prefix anyway so that a
-- client-side change which stops stripping it does not silently drop every row,
-- then enforce the 17-char Crockford base32 shape (digits and upper-case
-- letters). Malformed codes are dropped rather than counted: nothing downstream
-- validates the code before firefox.com ingests the CSV.
--
-- CHANNEL: all channels are counted and `normalized_channel` is kept as a
-- column so consumers can filter. As of 2026-08-18 only nightly has ever
-- reported — the instrumentation targets 155 and had not reached release — so
-- filtering to release here would make this table permanently empty and remove
-- the only way to validate the pipeline end to end. Revisit before the referral
-- hub is public: nightly test installs should not inflate a user-facing count.
--
-- GRAIN / DEDUPE: one row per (submission_date, invite_code, normalized_channel),
-- counting distinct client_ids. The ping fires once per profile, so within a
-- partition COUNT(DISTINCT client_id) is exact. KNOWN LIMITATION: a client whose
-- upload is retried across a midnight boundary would be counted in two
-- partitions and so twice in the cumulative total. Accepted for the MVP as
-- vanishingly rare; the durable fix is a client-level first-seen table, tracked
-- as a follow-up rather than paid for on every run here.
WITH referred_clients AS (
  -- One row per referred client per platform, un-aggregated, so a code used on
  -- both desktop and Android collapses to a single row below rather than one row
  -- per platform. Desktop and Fenix client_id namespaces are disjoint.
  SELECT
    metrics.string.browser_referral_code AS raw_code,
    normalized_channel,
    client_info.client_id AS client_id,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.referrals`
  WHERE
    DATE(submission_timestamp) = @submission_date
  UNION ALL
  SELECT
    metrics.string.referrals_code AS raw_code,
    normalized_channel,
    client_info.client_id AS client_id,
  FROM
    `moz-fx-data-shared-prod.fenix.referrals`
  WHERE
    DATE(submission_timestamp) = @submission_date
)
SELECT
  @submission_date AS submission_date,
  REGEXP_REPLACE(raw_code, r'^fxrefer', '') AS invite_code,
  normalized_channel,
  COUNT(DISTINCT client_id) AS install_count,
FROM
  referred_clients
WHERE
  REGEXP_CONTAINS(REGEXP_REPLACE(raw_code, r'^fxrefer', ''), r'^[A-Z0-9]{17}$')
GROUP BY
  invite_code,
  normalized_channel
