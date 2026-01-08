-- TODO: normalized_channel needs to be added to the results.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.amo_glean.addons`
AS
-- TODO: should we include channel information?
SELECT
  submission_timestamp,
  client_info.client_id,
  sample_id,
  metrics.object.addons_active_addons,
  metrics.object.addons_theme,
  normalized_country_code,
  client_info.locale,
  normalized_os,
  -- TODO: do we actually still need this regex for Glean pings?
  -- Accepts formats: 80.0 80.0.0 80.0.0a1 80.0.0b1
  IF(
    REGEXP_CONTAINS(client_info.app_display_version, r'^(\d+\.\d+(\.\d+)?([ab]\d+)?)$'),
    client_info.app_display_version,
    NULL
  ) AS app_version,
  CAST(NULL AS STRING) AS legacy_telemetry_client_id, -- only exists in firefox_desktop
  "fenix" AS app_name,
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.addons`
UNION ALL
SELECT
  submission_timestamp,
  client_info.client_id,
  sample_id,
  metrics.object.addons_active_addons,
  metrics.object.addons_theme,
  normalized_country_code,
  client_info.locale,
  normalized_os,
  -- Accepts formats: 80.0 80.0.0 80.0.0a1 80.0.0b1
  IF(
    REGEXP_CONTAINS(client_info.app_display_version, r'^(\d+\.\d+(\.\d+)?([ab]\d+)?)$'),
    client_info.app_display_version,
    NULL
  ) AS app_version,
  CAST(NULL AS STRING) AS legacy_telemetry_client_id, -- only exists in firefox_desktop
  "fenix" AS app_name,
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.addons`
UNION ALL
SELECT
  submission_timestamp,
  client_info.client_id,
  sample_id,
  metrics.object.addons_active_addons,
  metrics.object.addons_theme,
  normalized_country_code,
  client_info.locale,
  normalized_os,
  -- Accepts formats: 80.0 80.0.0 80.0.0a1 80.0.0b1
  IF(
    REGEXP_CONTAINS(client_info.app_display_version, r'^(\d+\.\d+(\.\d+)?([ab]\d+)?)$'),
    client_info.app_display_version,
    NULL
  ) AS app_version,
  CAST(NULL AS STRING) AS legacy_telemetry_client_id, -- only exists in firefox_desktop
  "fenix" AS app_name,
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix.addons`
UNION ALL
SELECT
  submission_timestamp,
  client_info.client_id,
  sample_id,
  metrics.object.addons_active_addons,
  metrics.object.addons_theme,
  normalized_country_code,
  client_info.locale,
  normalized_os,
  -- Accepts formats: 80.0 80.0.0 80.0.0a1 80.0.0b1
  IF(
    REGEXP_CONTAINS(client_info.app_display_version, r'^(\d+\.\d+(\.\d+)?([ab]\d+)?)$'),
    client_info.app_display_version,
    NULL
  ) AS app_version,
  CAST(NULL AS STRING) AS legacy_telemetry_client_id, -- only exists in firefox_desktop
  "fenix" AS app_name,
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.addons`
UNION ALL
SELECT
  submission_timestamp,
  client_info.client_id,
  sample_id,
  metrics.object.addons_active_addons,
  metrics.object.addons_theme,
  normalized_country_code,
  client_info.locale,
  normalized_os,
  -- Accepts formats: 80.0 80.0.0 80.0.0a1 80.0.0b1
  IF(
    REGEXP_CONTAINS(client_info.app_display_version, r'^(\d+\.\d+(\.\d+)?([ab]\d+)?)$'),
    client_info.app_display_version,
    NULL
  ) AS app_version,
  CAST(NULL AS STRING) AS legacy_telemetry_client_id, -- only exists in firefox_desktop
  "fenix" AS app_name,
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.addons`
-- Firefox Desktop
UNION ALL
SELECT
  submission_timestamp,
  client_info.client_id,
  sample_id,
  metrics.object.addons_active_addons,
  metrics.object.addons_theme,
  normalized_country_code,
  client_info.locale,
  normalized_os,
  -- Accepts formats: 80.0 80.0.0 80.0.0a1 80.0.0b1
  IF(
    REGEXP_CONTAINS(client_info.app_display_version, r'^(\d+\.\d+(\.\d+)?([ab]\d+)?)$'),
    client_info.app_display_version,
    NULL
  ) AS app_version,
  metrics.uuid.legacy_telemetry_client_id,
  "firefox_desktop" AS app_name,
FROM
  `moz-fx-data-shared-prod.firefox_desktop.addons`
