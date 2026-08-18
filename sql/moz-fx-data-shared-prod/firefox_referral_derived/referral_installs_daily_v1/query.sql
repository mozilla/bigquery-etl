-- Daily count of Firefox first_run installs per referral (invite) code.
--
-- Source: the dedicated Glean `referrals` ping, sent once on first run by a
-- profile carrying referral attribution data — `browser.referral_code` on
-- desktop (bug 2055255), `referrals.code` on Android (bug 2062793).
--
-- Do NOT try to read the code from GA4 `utm_content` or from
-- `attribution.content`. Firefox strips it out of regular attribution data by
-- design — that is the point of bug 2055255 — so an earlier version of this
-- query which used that path returned zero rows every day from launch
-- (DENG-11237).
--
-- See metadata.yaml for the grain, channel and dedupe notes.
WITH referred_clients AS (
  -- One row per referred client per platform, un-aggregated, so the aggregation
  -- below collapses desktop and Android together within a channel. Desktop and
  -- Fenix client_id namespaces are disjoint, so the distinct count never
  -- double-counts across platforms.
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
),
codes AS (
  -- Stripped after the UNION rather than in each branch above, so the prefix is
  -- removed in exactly one place and the value validated below is the same value
  -- that gets emitted.
  --
  -- The ping already carries the code bare (the client strips `fxrefer` when it
  -- extracts it), so this is belt-and-braces: if a client-side change ever stops
  -- stripping it, rows keep flowing instead of silently vanishing.
  SELECT
    REGEXP_REPLACE(raw_code, r'^fxrefer', '') AS invite_code,
    normalized_channel,
    client_id,
  FROM
    referred_clients
)
-- normalized_channel is last to match the destination table, which already
-- exists as (submission_date, invite_code, install_count).
SELECT
  @submission_date AS submission_date,
  invite_code,
  COUNT(DISTINCT client_id) AS install_count,
  normalized_channel,
FROM
  codes
WHERE
  -- Drop anything that is not a well-formed code. The ping is a free-form string
  -- metric, so a malformed value is possible in practice: a client-side bug, a
  -- hand-edited attribution code, or a truncated value all arrive here as data.
  -- Nothing downstream validates the code before firefox.com ingests the CSV, so
  -- this is the only gate.
  --
  -- Expected shape is literally 17 chars of [A-Z0-9] — this is deliberately a
  -- little looser than Crockford base32, which also excludes I/L/O/U; the extra
  -- strictness would buy nothing and would break if the generator changed.
  --
  -- CASE: every code observed so far is upper case, and Crockford is
  -- case-insensitive by spec, so a lower-case code would be valid to the
  -- Websites team but dropped here. Left strict rather than upper-casing,
  -- because the CSV is matched against Springfield's stored codes and silently
  -- changing case could mismatch. Confirm with the Websites team; if lower case
  -- is possible, normalise here rather than widening the charset.
  REGEXP_CONTAINS(invite_code, r'^[A-Z0-9]{17}$')
GROUP BY
  invite_code,
  normalized_channel
