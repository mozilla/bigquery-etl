CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.remote_settings_uptake_live`
AS
SELECT
  submission_timestamp,
  SAFE_CAST(mozfun.norm.truncate_version(client_info.app_display_version, 'major') AS INTEGER) AS major_version,
  client_info.client_id AS client_id,
  'desktop' AS platform,
  normalized_channel,
  normalized_os,
  normalized_os_version,
  normalized_country_code,
  -- Extra attributes
  -- See https://searchfox.org/firefox-main/rev/1427c88632d1474d/services/common/metrics.yaml
  mozfun.map.get_key(e.extra, 'value') AS extra_status, -- It's 'status' in our uptake.
  mozfun.map.get_key(e.extra, 'trigger') AS extra_trigger,
  mozfun.map.get_key(e.extra, 'source') AS extra_source,
  mozfun.map.get_key(e.extra, 'errorName') AS extra_errorname,
  mozfun.map.get_key(e.extra, 'timestamp') AS extra_timestamp,
  SAFE_CAST(mozfun.map.get_key(e.extra, 'age') AS INT64) AS extra_age,
  SAFE_CAST(mozfun.map.get_key(e.extra, 'duration') AS INT64) AS extra_duration,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_live.events_v1`
INNER JOIN UNNEST(events) AS e ON
  e.category = 'uptake.remotecontent.result'
  AND e.name = 'uptake_remotesettings'
