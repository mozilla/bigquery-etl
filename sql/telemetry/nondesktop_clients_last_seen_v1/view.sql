CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.nondesktop_clients_last_seen_v1`
AS
-- For context on naming and channels of Fenix apps, see:
-- https://docs.google.com/document/d/1Ym4eZyS0WngEP6WdwJjmCoxtoQbJSvORxlQwZpuSV2I/edit#heading=h.69hvvg35j8un
WITH fenix_union AS (
  SELECT
    *,
    'org_mozilla_fenix' AS _dataset
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'org_mozilla_fenix_nightly' AS _dataset
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'org_mozilla_firefox' AS _dataset
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'org_mozilla_firefox_beta' AS _dataset
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'org_mozilla_fennec_aurora' AS _dataset
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.baseline_clients_last_seen`
),
fenix_app_info AS (
  SELECT
    *,
    mozfun.norm.fenix_app_info(_dataset, app_build) AS _app_info
  FROM
    fenix_union
),
fenix_normalized AS (
  SELECT
    * EXCEPT (_dataset, _app_info) REPLACE(_app_info.channel AS normalized_channel),
    _app_info.app_name,
  FROM
    fenix_app_info
),
fennec_ios_union AS (
  SELECT
    * REPLACE ('release' AS normalized_channel)
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox.baseline_clients_last_seen`
  UNION ALL
  SELECT
    * REPLACE ('beta' AS normalized_channel)
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.baseline_clients_last_seen`
  UNION ALL
  SELECT
    * REPLACE ('nightly' AS normalized_channel)
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec.baseline_clients_last_seen`
),
fennec_ios_normalized AS (
  SELECT
    *,
    'Fennec' AS app_name,
  FROM
    fennec_ios_union
  WHERE
    -- Firefox for iOS first introduced Glean in version 26.0 and we consider
    -- baseline pings to be the source of truth for usage starting in version 28.0;
    -- for older versions, we rely on core pings.
    -- See https://github.com/mozilla/gud/issues/117
    SAFE_CAST(SPLIT(app_display_version, '.')[SAFE_OFFSET(0)] AS INT64) >= 28
),
glean_union AS (
  SELECT
    *
  FROM
    fenix_normalized
  UNION ALL
  SELECT
    *
  FROM
    fennec_ios_normalized
  UNION ALL
  SELECT
    *,
    'Lockwise Baseline' AS app_name,
  FROM
    `moz-fx-data-shared-prod.mozilla_lockbox.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'Lockwise Baseline' AS app_name,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_lockbox.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'Reference Browser Baseline' AS app_name,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_reference_browser.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'Firefox TV Baseline' AS app_name,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_tv_firefox.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'VR Browser Baseline' AS app_name,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_vrbrowser.baseline_clients_last_seen`
),
core_normalized AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry.core_clients_last_seen`
  WHERE
  -- Glean baseline pings are considered source of truth for Firefox for iOS usage from
  -- version 28.0, so we only consider core pings if they come from an earlier version
    NOT (
      app_name = 'Fennec'
      AND os = 'iOS'
      AND SAFE_CAST(SPLIT(metadata_app_version, '.')[SAFE_OFFSET(0)] AS INT64) >= 28
    )
)
SELECT
  submission_date,
  client_id,
  days_seen_bits,
  days_since_seen,
  days_created_profile_bits,
  days_since_created_profile,
  app_name,
  os,
  osversion AS os_version,
  normalized_channel,
  campaign,
  country,
  locale,
  distribution_id,
  metadata_app_version AS app_version,
  device AS device_model,
FROM
  core_normalized
UNION ALL
SELECT
  submission_date,
  client_id,
  days_seen_bits,
  days_since_seen,
  days_created_profile_bits,
  days_since_created_profile,
  app_name,
  normalized_os AS os,
  normalized_os_version AS os_version,
  normalized_channel,
  NULL AS campaign,
  country,
  locale,
  NULL AS distribution_id,
  app_display_version AS app_version,
  device_model,
FROM
  glean_union
