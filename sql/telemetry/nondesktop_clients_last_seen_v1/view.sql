CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.nondesktop_clients_last_seen_v1` AS
-- For context on naming and channels of Fenix apps, see:
-- https://docs.google.com/document/d/1Ym4eZyS0WngEP6WdwJjmCoxtoQbJSvORxlQwZpuSV2I/edit#heading=h.69hvvg35j8un
WITH glean_union AS (
    SELECT
    * REPLACE('beta' AS normalized_channel),
    'Firefox Preview' AS app_name,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix.baseline_clients_last_seen`
  UNION ALL
  SELECT
    * REPLACE('nightly' AS normalized_channel),
    'Firefox Preview' AS app_name,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.baseline_clients_last_seen`
  UNION ALL
  SELECT
    * REPLACE('release' AS normalized_channel),
    'Fenix' AS app_name,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox.baseline_clients_last_seen`
  UNION ALL
  SELECT
    * REPLACE('beta' AS normalized_channel),
    'Fenix' AS app_name,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta.baseline_clients_last_seen`
  UNION ALL
  SELECT
    * REPLACE('nightly' AS normalized_channel),
    'Fenix' AS app_name,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.baseline_clients_last_seen`
  UNION ALL
  SELECT
    * REPLACE('release' AS normalized_channel),
    'VR Browser' AS app_name,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_vrbrowser.baseline_clients_last_seen`
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
  metadata_app_version AS app_version
FROM
  `moz-fx-data-shared-prod.telemetry.core_clients_last_seen`
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
  app_display_version AS app_version
FROM
  glean_union
