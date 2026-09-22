--
-- Query for remote_settings_derived.clients_uptake_events_v1
--
WITH today_all_implementations AS (
    --
    -- Desktop (Gecko / Rust)
    --
  SELECT
    submission_timestamp,
    client_info,
    normalized_app_id,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    normalized_country_code,
    event_extra,
    -- As of 2026-09-22, desktop telemetry for rust is not enabled, but could land anytime.
    IF(event_category = 'remote_settings', 'rust', 'gecko') AS implementation,
    'desktop' AS platform
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.events_unnested`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_name = 'uptake_remotesettings'
    AND (event_category = 'uptake.remotecontent.result' OR event_category = 'remote_settings')
  UNION ALL
    --
    -- Android (Gecko / Rust)
    --
  SELECT
    submission_timestamp,
    client_info,
    normalized_app_id,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    normalized_country_code,
    event_extra,
    IF(event_category = 'remote_settings', 'rust', 'gecko') AS implementation,
    'android' AS platform
  FROM
    `moz-fx-data-shared-prod.fenix.events_unnested`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_name = 'uptake_remotesettings'
    AND (event_category = 'uptake.remotecontent.result' OR event_category = 'remote_settings')
  UNION ALL
    --
    -- iOS (Rust)
    --
  SELECT
    submission_timestamp,
    client_info,
    normalized_app_id,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    normalized_country_code,
    event_extra,
    'rust' AS implementation,
    'ios' AS platform
  FROM
    `moz-fx-data-shared-prod.firefox_ios.events_unnested`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_category = 'remote_settings'
    AND event_name = 'uptake_remotesettings'
)
SELECT
  submission_timestamp,
  SAFE_CAST(
    mozfun.norm.truncate_version(client_info.app_display_version, 'major') AS INT64
  ) AS major_version,
  client_info.client_id,
  implementation,
  platform,
  normalized_app_id,
  normalized_channel,
  normalized_os,
  normalized_os_version,
  normalized_country_code,
  mozfun.map.get_key(event_extra, 'value') AS extra_status,
  mozfun.map.get_key(event_extra, 'trigger') AS extra_trigger,
  mozfun.map.get_key(event_extra, 'source') AS extra_source,
  mozfun.map.get_key(event_extra, 'errorName') AS extra_errorname,
  mozfun.map.get_key(event_extra, 'timestamp') AS extra_timestamp,
  SAFE_CAST(mozfun.map.get_key(event_extra, 'age') AS DECIMAL) AS extra_age,
  SAFE_CAST(mozfun.map.get_key(event_extra, 'duration') AS INT64) AS extra_duration
FROM
  today_all_implementations
