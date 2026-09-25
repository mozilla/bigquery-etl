{% set uptake_filter = "(e.category = 'uptake.remotecontent.result' OR e.category = 'remote_settings') AND e.name = 'uptake_remotesettings'" %}
{% set android_apps = [('org_mozilla_firefox', 'release'), ('org_mozilla_firefox_beta', 'beta'), ('org_mozilla_fenix', 'nightly')] %}
{% set ios_apps = [('org_mozilla_ios_firefox', 'release'), ('org_mozilla_ios_firefoxbeta', 'beta'), ('org_mozilla_ios_fennec', 'nightly')] %}
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.remote_settings_uptake_live`
AS
WITH all_implementations AS (
  -- Desktop (Gecko / Rust): a single dataset covers every channel.
  SELECT
    'desktop' AS platform,
    CONCAT('org_mozilla_desktop_firefox_', normalized_channel) AS normalized_app_id,
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
    ON {{ uptake_filter }}
    {% for app, channel in android_apps %}
      UNION ALL
  -- Android (Gecko / Rust): one live dataset per channel.
      SELECT
        'android' AS platform,
        '{{ app }}' AS normalized_app_id,
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
        `moz-fx-data-shared-prod.{{ app }}_live.events_v1`
      INNER JOIN
        UNNEST(events) AS e
        ON {{ uptake_filter }}
    {% endfor %}
    {% for app, channel in ios_apps %}
      UNION ALL
  -- iOS (Rust): one live dataset per channel, each mapping to a single channel.
      SELECT
        'ios' AS platform,
        '{{ app }}' AS normalized_app_id,
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
        `moz-fx-data-shared-prod.{{ app }}_live.events_v1`
      INNER JOIN
        UNNEST(events) AS e
        ON {{ uptake_filter }}
    {% endfor %}
)
SELECT
  submission_timestamp,
  platform,
  implementation,
  normalized_app_id,
  SAFE_CAST(mozfun.norm.truncate_version(app_display_version, 'major') AS INTEGER) AS major_version,
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
