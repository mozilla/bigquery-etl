{{ header }}

WITH base AS (
  SELECT
    submission_timestamp,
    DATE(submission_timestamp) AS submission_date,
    metrics.uuid.usage_profile_id,
    normalized_channel,
    client_info.app_display_version,
    client_info.app_build,
    normalized_os,
    normalized_os_version,
    client_info.locale,
    {% if has_distribution_id %}
    metrics.string.metrics_distribution_id AS distribution_id,
    {% else %}
    CAST(NULL AS STRING) AS distribution_id,
    {% endif %}
    {% if "_desktop" in app_name %}
    metrics.counter.browser_engagement_uri_count,
    metrics.counter.browser_engagement_active_ticks,
    {% endif %}
    CAST(NULL AS BOOLEAN) AS is_active,
    SAFE.PARSE_DATE('%F', SUBSTR(client_info.first_run_date, 1, 10)) AS first_run_date,
  FROM
    `{{ project_id }}.{{ dau_reporting_stable_table }}`
  WHERE
    usage_profile_id IS NOT NULL
)
SELECT
  submission_date,
  usage_profile_id,
  --
  -- Take the earliest first_run_date if ambiguous.
  MIN(first_run_date) OVER w1 AS first_run_date,
  -- For all other dimensions, we use the mode of observed values in the day.
  udf.mode_last(ARRAY_AGG(normalized_channel) OVER w1) AS normalized_channel,
  udf.mode_last(ARRAY_AGG(normalized_os) OVER w1) AS normalized_os,
  udf.mode_last(ARRAY_AGG(normalized_os_version) OVER w1) AS normalized_os_version,
  udf.mode_last(ARRAY_AGG(locale) OVER w1) AS locale,
  udf.mode_last(ARRAY_AGG(app_build) OVER w1) AS app_build,
  udf.mode_last(ARRAY_AGG(app_display_version) OVER w1) AS app_display_version,
  udf.mode_last(ARRAY_AGG(distribution_id) OVER w1) AS distribution_id,
  {% if "_desktop" in app_name %}
  COALESCE(is_active, SUM(browser_engagement_uri_count) > 0 AND SUM(browser_engagement_active_ticks) > 0, False) AS is_active,
  {% else %}
  -- At the moment we do not have duration, default to True.
  -- COALESCE(is_active, SUM(IF(duration BETWEEN 0 AND 100000, duration, 0)) OVER w1 > 0, False) AS is_active,
  TRUE AS is_active
  {% endif %}
FROM
  base
WHERE
  {% raw %}
  {% if is_init() %}
    submission_date >= '2024-10-10'
  {% else %}
    submission_date = @submission_date
  {% endif %}
  {% endraw %}
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      usage_profile_id,
      submission_date
    ORDER BY
      submission_timestamp
  ) = 1

WINDOW
  w1 AS (
    PARTITION BY
      usage_profile_id,
      submission_date
    ORDER BY
      submission_timestamp
    ROWS BETWEEN
      UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  )
