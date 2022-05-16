SELECT
  client_id,
  first_seen_date AS cohort_date,
  ANY_VALUE(activity_segment) AS activity_segment,
  ANY_VALUE(app_version) AS app_version,
  ANY_VALUE(attribution_campaign) AS attribution_campaign,
  ANY_VALUE(attribution_content) AS attribution_content,
  ANY_VALUE(attribution_experiment) AS attribution_experiment,
  ANY_VALUE(attribution_medium) AS attribution_medium,
  ANY_VALUE(attribution_source) AS attribution_source,
  ANY_VALUE(attribution_variation) AS attribution_variation,
  ANY_VALUE(city) AS city,
  ANY_VALUE(country) AS country,
  ANY_VALUE(device_model) AS device_model,
  ANY_VALUE(distribution_id) AS distribution_id,
  ANY_VALUE(is_default_browser) AS is_default_browser,
  ANY_VALUE(locale) AS locale,
  ANY_VALUE(normalized_app_name) AS normalized_app_name,
  ANY_VALUE(normalized_channel) AS normalized_channel,
  ANY_VALUE(normalized_os) AS normalized_os,
  ANY_VALUE(normalized_os_version) AS normalized_os_version,
  ANY_VALUE(os_version_major) AS os_version_major,
  ANY_VALUE(os_version_minor) AS os_version_minor,
FROM
  telemetry_derived.unified_metrics_v1
WHERE
  submission_date = @submission_date
  AND first_seen_date = @submission_date
GROUP BY
  client_id,
  first_seen_date
