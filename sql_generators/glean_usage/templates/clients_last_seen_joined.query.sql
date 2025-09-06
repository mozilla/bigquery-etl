WITH baseline AS (
  SELECT
    {% if app_name == "firefox_desktop" %}
      days_since_desktop_active,
    {% endif %}
    days_since_seen,
    days_since_active,
    days_since_created_profile,
    days_since_seen_session_start,
    days_since_seen_session_end,
    days_since_visited_1_uri,
    days_seen_bits,
    days_created_profile_bits,
    submission_date,
    client_id,
    sample_id,
    first_run_date,
    durations,
    days_seen_session_start_bits,
    days_seen_session_end_bits,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    android_sdk_version,
    locale,
    city,
    country,
    app_build,
    app_channel,
    app_display_version,
    architecture,
    device_manufacturer,
    device_model,
    telemetry_sdk_build,
    first_seen_date,
    is_new_profile,
    isp,
    days_active_bits,
    distribution_id,
    geo_subdivision,
    install_source,
    {% if app_name == "firefox_desktop" %}
      days_desktop_active_bits,
    {% endif %}
    windows_build_number,
    browser_engagement_uri_count,
    browser_engagement_active_ticks,
    legacy_telemetry_client_id,
    is_default_browser,
    attribution,
    `distribution`,
    attribution_dltoken,
    attribution_dlsource,
    attribution_experiment,
    attribution_variation,
    attribution_ua,
    profile_group_id AS baseline_profile_group_id
  FROM
    `{{ project_id }}.{{ app_name }}.baseline_clients_last_seen`
  WHERE
    submission_date = @submission_date
),
metrics AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    normalized_channel,
    n_metrics_ping,
    days_sent_metrics_ping_bits,
    {% if app_name == "firefox_desktop" %}
      profile_group_id
    {% endif %}
  FROM
    `{{ project_id }}.{{ app_name }}.metrics_clients_last_seen`
  WHERE
    -- The join between baseline and metrics pings is based on submission_date with a 1 day delay,
    -- since metrics pings usually arrive within 1 day after their logical activity period.
    submission_date = DATE_ADD(@submission_date, INTERVAL 1 DAY)
)
SELECT
  baseline.client_id,
  baseline.sample_id,
  baseline.submission_date,
  baseline.normalized_channel,
  {% if app_name == "firefox_desktop" %}
    baseline.days_since_desktop_active,
  {% endif %}
  baseline.days_since_seen,
  baseline.days_since_active,
  baseline.days_since_created_profile,
  baseline.days_since_seen_session_start,
  baseline.days_since_seen_session_end,
  baseline.days_since_visited_1_uri,
  baseline.days_seen_bits,
  baseline.days_created_profile_bits,
  baseline.first_run_date,
  baseline.durations,
  baseline.days_seen_session_start_bits,
  baseline.days_seen_session_end_bits,
  baseline.normalized_os,
  baseline.normalized_os_version,
  baseline.android_sdk_version,
  baseline.locale,
  baseline.city,
  baseline.country,
  baseline.app_build,
  baseline.app_channel,
  baseline.app_display_version,
  baseline.architecture,
  baseline.device_manufacturer,
  baseline.device_model,
  baseline.telemetry_sdk_build,
  baseline.first_seen_date,
  baseline.is_new_profile,
  baseline.isp,
  baseline.days_active_bits,
  baseline.distribution_id,
  baseline.geo_subdivision,
  baseline.install_source,
  {% if app_name == "firefox_desktop" %}
    baseline.days_desktop_active_bits,
  {% endif %}
  baseline.windows_build_number,
  baseline.browser_engagement_uri_count,
  baseline.browser_engagement_active_ticks,
  baseline.legacy_telemetry_client_id,
  baseline.attribution,
  baseline.`distribution`,
  baseline.attribution_dltoken,
  baseline.attribution_dlsource,
  baseline.attribution_experiment,
  baseline.attribution_variation,
  baseline.attribution_ua,
  baseline.baseline_profile_group_id,
  metrics.n_metrics_ping,
  metrics.days_sent_metrics_ping_bits,
  {% if app_name == "firefox_desktop" %}
    metrics.profile_group_id,
  {% endif %}
  baseline.is_default_browser
FROM
  baseline
LEFT JOIN
  metrics
  ON baseline.client_id = metrics.client_id
  AND baseline.sample_id = metrics.sample_id
  AND (
    baseline.normalized_channel = metrics.normalized_channel
    OR (baseline.normalized_channel IS NULL AND metrics.normalized_channel IS NULL)
  )
-- In some rare cases we end up with the same client_id in multiple channels
-- In those cases, this union can result in client_id duplicates.
-- This logic ensures that the resulting table only includes the client from the channel
-- we've seen first.
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY first_seen_date ASC) = 1
