--
-- Query for remote_settings_derived.clients_uptake_events_v1
--
-- As of 2026-09-22, desktop telemetry for rust is not enabled, but could land anytime.
--
{% set uptake_filter = "event_name = 'uptake_remotesettings' AND event_category IN ('uptake.remotecontent.result', 'remote_settings')" %}
{% set sources = [('desktop', 'firefox_desktop'), ('android', 'fenix'), ('ios', 'firefox_ios')] %}
{% for platform, dataset in sources %}
  {% if not loop.first %}
    UNION ALL
  {% endif %}
  SELECT
    submission_timestamp,
    sample_id,
    app_version_major,
    client_id,
    IF(event_category = 'remote_settings', 'rust', 'gecko') AS implementation,
    '{{ platform }}' AS platform,
    normalized_app_id,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    normalized_country_code,
    LAX_STRING(event_extra.value) AS extra_status,
    LAX_STRING(event_extra.trigger) AS extra_trigger,
    LAX_STRING(event_extra.source) AS extra_source,
    LAX_STRING(event_extra.errorName) AS extra_errorname,
    LAX_STRING(event_extra.timestamp) AS extra_timestamp,
    LAX_FLOAT64(event_extra.age) AS extra_age,
    LAX_INT64(event_extra.duration) AS extra_duration
  FROM
    `moz-fx-data-shared-prod.{{ dataset }}.events_stream`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND {{ uptake_filter }}
{% endfor %}
