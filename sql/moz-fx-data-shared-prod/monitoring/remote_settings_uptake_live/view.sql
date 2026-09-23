CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.remote_settings_uptake_live`
AS
WITH all_implementations AS (
-- Desktop (Gecko / Rust) -- one dataset covers every channel
  SELECT
    'desktop' AS platform,
    'firefox_desktop' AS normalized_app_id,
    IF(e.category = 'remote_settings', 'rust', 'gecko') AS implementation,
    submission_timestamp,
    client_info.client_id AS client_id,
    client_info.app_display_version AS app_display_version,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    normalized_country_code,
    e.extra AS event_extra,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.events_v1`
  INNER JOIN
    UNNEST(events) AS e
    ON (e.category = 'uptake.remotecontent.result' OR e.category = 'remote_settings')
    AND e.name = 'uptake_remotesettings'
  UNION ALL
-- Android (Gecko / Rust) -- release
  SELECT
    'android' AS platform,
    'org_mozilla_firefox' AS normalized_app_id,
    IF(e.category = 'remote_settings', 'rust', 'gecko') AS implementation,
    submission_timestamp,
    client_info.client_id AS client_id,
    client_info.app_display_version AS app_display_version,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    normalized_country_code,
    e.extra AS event_extra,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_live.events_v1`
  INNER JOIN
    UNNEST(events) AS e
    ON (e.category = 'uptake.remotecontent.result' OR e.category = 'remote_settings')
    AND e.name = 'uptake_remotesettings'
  UNION ALL
-- Android (Gecko / Rust) -- beta
  SELECT
    'android' AS platform,
    'org_mozilla_firefox_beta' AS normalized_app_id,
    IF(e.category = 'remote_settings', 'rust', 'gecko') AS implementation,
    submission_timestamp,
    client_info.client_id AS client_id,
    client_info.app_display_version AS app_display_version,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    normalized_country_code,
    e.extra AS event_extra,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_live.events_v1`
  INNER JOIN
    UNNEST(events) AS e
    ON (e.category = 'uptake.remotecontent.result' OR e.category = 'remote_settings')
    AND e.name = 'uptake_remotesettings'
  UNION ALL
-- Android (Gecko / Rust) -- nightly
  SELECT
    'android' AS platform,
    'org_mozilla_fenix' AS normalized_app_id,
    IF(e.category = 'remote_settings', 'rust', 'gecko') AS implementation,
    submission_timestamp,
    client_info.client_id AS client_id,
    client_info.app_display_version AS app_display_version,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    normalized_country_code,
    e.extra AS event_extra,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_live.events_v1`
  INNER JOIN
    UNNEST(events) AS e
    ON (e.category = 'uptake.remotecontent.result' OR e.category = 'remote_settings')
    AND e.name = 'uptake_remotesettings'
  UNION ALL
-- iOS (Rust) -- release
  SELECT
    'ios' AS platform,
    'org_mozilla_ios_firefox' AS normalized_app_id,
    'rust' AS implementation,
    submission_timestamp,
    client_info.client_id AS client_id,
    client_info.app_display_version AS app_display_version,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    normalized_country_code,
    e.extra AS event_extra,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_live.events_v1`
  INNER JOIN
    UNNEST(events) AS e
    ON e.category = 'remote_settings'
    AND e.name = 'uptake_remotesettings'
  UNION ALL
-- iOS (Rust) -- beta
  SELECT
    'ios' AS platform,
    'org_mozilla_ios_firefoxbeta' AS normalized_app_id,
    'rust' AS implementation,
    submission_timestamp,
    client_info.client_id AS client_id,
    client_info.app_display_version AS app_display_version,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    normalized_country_code,
    e.extra AS event_extra,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_live.events_v1`
  INNER JOIN
    UNNEST(events) AS e
    ON e.category = 'remote_settings'
    AND e.name = 'uptake_remotesettings'
  UNION ALL
-- iOS (Rust) -- nightly
  SELECT
    'ios' AS platform,
    'org_mozilla_ios_fennec' AS normalized_app_id,
    'rust' AS implementation,
    submission_timestamp,
    client_info.client_id AS client_id,
    client_info.app_display_version AS app_display_version,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    normalized_country_code,
    e.extra AS event_extra,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec_live.events_v1`
  INNER JOIN
    UNNEST(events) AS e
    ON e.category = 'remote_settings'
    AND e.name = 'uptake_remotesettings'
)
SELECT
  submission_timestamp,
  platform,
  normalized_app_id,
  implementation,
  SAFE_CAST(
    mozfun.norm.truncate_version(app_display_version, 'major') AS INTEGER
  ) AS major_version,
  client_id,
  normalized_channel,
  normalized_os,
  normalized_os_version,
  normalized_country_code,
  -- Extra attributes
  -- See https://searchfox.org/firefox-main/rev/1427c88632d1474d/services/common/metrics.yaml
  mozfun.map.get_key(event_extra, 'value') AS extra_status, -- It's 'status' in our uptake.
  mozfun.map.get_key(event_extra, 'trigger') AS extra_trigger,
  mozfun.map.get_key(event_extra, 'source') AS extra_source,
  mozfun.map.get_key(event_extra, 'errorName') AS extra_errorname,
  mozfun.map.get_key(event_extra, 'timestamp') AS extra_timestamp,
  SAFE_CAST(mozfun.map.get_key(event_extra, 'age') AS DECIMAL) AS extra_age,
  SAFE_CAST(mozfun.map.get_key(event_extra, 'duration') AS INT64) AS extra_duration,
FROM
  all_implementations
