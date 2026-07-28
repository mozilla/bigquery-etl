-- Daily count of Firefox first_run installs per referral (invite) code.
--
-- The invite code arrives as `fxrefer:<code>` and is expected to be 17 chars
-- after the prefix is stripped (Website team, 2026-07-24). The prefix strip is
-- length-agnostic; a checks.sql warn flags any drift from 17 chars.
--
-- DESKTOP source = GA4 / download-attribution path (cross-platform: Windows + Mac).
--   Do NOT use `telemetry.install` — it is the Windows-only installer ping and
--   silently drops Mac/Linux. The GA4/dltoken path is the cross-platform source.
--
-- Proven chain (validated 2026-07 against test code TESTCODE01):
--   GA4 (utm_content=fxrefer:) -> dl_token_ga_attribution_lookup_v2 (on dl_token)
--     -> baseline_clients_first_seen_v1 -> installed client.
--   This is the `firefox_desktop_derived.cfs_ga4_attr_v1` join pattern.
--
-- STATUS:
--   * Desktop source is LOCKED — the GA4 / download-attribution path is the
--     accepted permanent source (confirmed in DENG-11237, 2026-07-24).
--   * Fenix (Android) half is BLOCKED pending the Play Store attribution
--     mechanism/field from Nathan (see commented UNION ALL below).
--   * US-only MVP; Linux/iOS/EU/UK are unattributable, so counts undercount by design.
WITH stub_attr AS (
  -- dl_token -> GA identifiers, one row per dl_token
  SELECT
    dl_token,
    ga_client_id,
    stub_session_id,
  FROM
    `moz-fx-data-shared-prod.stub_attribution_service_derived.dl_token_ga_attribution_lookup_v2`
  WHERE
    COALESCE(ga_client_id, '') <> ''
    AND COALESCE(stub_session_id, '') <> ''
    AND COALESCE(dl_token, '') <> ''
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY dl_token ORDER BY first_seen_date ASC) = 1
),
ga4 AS (
  -- GA4 session carrying the referral code in utm_content
  SELECT
    ga_client_id,
    stub_session_id,
    COALESCE(manual_content, first_content_from_event_params) AS content,
  FROM
    `moz-fx-data-shared-prod.telemetry.ga4_sessions_firefoxcom_mozillaorg_combined`
  LEFT JOIN
    UNNEST(all_reported_stub_session_ids) AS stub_session_id
  WHERE
    -- download precedes first-run; look back so the session is in range
    session_date >= DATE_SUB(@submission_date, INTERVAL 30 DAY)
    AND session_date <= @submission_date
    AND (manual_content LIKE 'fxrefer:%' OR first_content_from_event_params LIKE 'fxrefer:%')
),
desktop AS (
  SELECT
    REGEXP_REPLACE(ga4.content, r'^fxrefer:', '') AS invite_code,
    COUNT(DISTINCT cfs.client_id) AS install_count,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_first_seen_v1` AS cfs
  LEFT JOIN
    stub_attr
    ON JSON_VALUE(cfs.attribution_ext.dltoken) = stub_attr.dl_token
  LEFT JOIN
    ga4
    ON stub_attr.ga_client_id = ga4.ga_client_id
    AND stub_attr.stub_session_id = ga4.stub_session_id
  WHERE
    cfs.submission_date = @submission_date -- required partition filter
    AND cfs.first_seen_date = @submission_date -- clients first seen today
    AND ga4.content LIKE 'fxrefer:%' -- referred clients only
  GROUP BY
    invite_code
)
-- FENIX (Android) — BLOCKED. Once Nathan defines the field/ping, add:
--   UNION ALL
--   SELECT
--     REGEXP_REPLACE(play_store_attribution_content, r'^fxrefer:', '') AS invite_code,
--     COUNT(DISTINCT client_id) AS install_count
--   FROM `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1`
--   WHERE first_seen_date = @submission_date
--     AND play_store_attribution_content LIKE 'fxrefer:%'
--   GROUP BY invite_code
SELECT
  @submission_date AS submission_date,
  invite_code,
  install_count,
FROM
  desktop
