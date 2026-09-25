--
-- Query for remote_settings_derived.clients_uptake_events_v1
--
-- `fenix.events` and `firefox_ios.events` already UNION every channel and fix up
-- `normalized_channel`; `firefox_desktop.events` is a single dataset covering all
-- channels. Only the desktop view lacks `normalized_app_id`, so it is set literally.
-- As of 2026-09-22, desktop telemetry for rust is not enabled, but could land anytime.
{% set uptake_filter = "e.name = 'uptake_remotesettings' AND (e.category = 'uptake.remotecontent.result' OR e.category = 'remote_settings')" %}
{% set sources = [('desktop', 'firefox_desktop'), ('android', 'fenix'), ('ios', 'firefox_ios')] %}
WITH today_all_implementations AS (
  {% for platform, dataset in sources %}
    {% if not loop.first %}
      UNION ALL
    {% endif %}
    SELECT
      submission_timestamp,
      client_info,
      {% if platform == 'desktop' %}
        'firefox_desktop' AS normalized_app_id,
      {% else %}
        normalized_app_id,
      {% endif %}
      normalized_channel,
      normalized_os,
      normalized_os_version,
      normalized_country_code,
      e.extra AS event_extra,
      IF(e.category = 'remote_settings', 'rust', 'gecko') AS implementation,
      '{{ platform }}' AS platform
    FROM
      `moz-fx-data-shared-prod.{{ dataset }}.events`
    INNER JOIN
      UNNEST(events) AS e
      ON {{ uptake_filter }}
    WHERE
      DATE(submission_timestamp) = @submission_date
  {% endfor %}
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
