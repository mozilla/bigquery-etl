{{ header }}

WITH usage_reporting_base AS (
  SELECT
    submission_timestamp,
    DATE(submission_timestamp) AS submission_date,
    metrics.uuid.usage_profile_id,
    normalized_channel,
    -- client_info.app_display_version,
    -- client_info.app_build,
    normalized_os,
    normalized_os_version,
    -- client_info.locale,
    normalized_country_code,
    {% if has_distribution_id %}
    metrics.string.metrics_distribution_id AS distribution_id,
    {% else %}
    CAST(NULL AS STRING) AS distribution_id,
    {% endif %}
    {% if "_desktop" in app_name %}
    COALESCE(metrics.counter.browser_engagement_uri_count, 0) AS browser_engagement_uri_count,
    COALESCE(metrics.counter.browser_engagement_active_ticks, 0) AS browser_engagement_active_ticks,
    {% endif %}
    CAST(NULL AS BOOLEAN) AS is_active,
    -- SAFE.PARSE_DATE('%F', SUBSTR(client_info.first_run_date, 1, 10)) AS first_run_date,
  FROM
    `{{ project_id }}.{{ usage_reporting_stable_table }}`
  WHERE
    {% raw %}
    {% if is_init() %}
    DATE(submission_timestamp) >= '2024-10-10'
    {% else %}
    DATE(submission_timestamp) = @submission_date
    {% endif %}
    {% endraw %}
    AND metrics.uuid.usage_profile_id IS NOT NULL
),
dau_reporting_base AS (
  SELECT
    submission_timestamp,
    DATE(submission_timestamp) AS submission_date,
    metrics.uuid.usage_profile_id,
    normalized_channel,
    -- client_info.app_display_version,
    -- client_info.app_build,
    normalized_os,
    normalized_os_version,
    -- client_info.locale,
    normalized_country_code,
    {% if has_distribution_id %}
    metrics.string.metrics_distribution_id AS distribution_id,
    {% else %}
    CAST(NULL AS STRING) AS distribution_id,
    {% endif %}
    {% if "_desktop" in app_name %}
    COALESCE(metrics.counter.browser_engagement_uri_count, 0) AS browser_engagement_uri_count,
    COALESCE(metrics.counter.browser_engagement_active_ticks, 0) AS browser_engagement_active_ticks,
    {% endif %}
    CAST(NULL AS BOOLEAN) AS is_active,
    -- SAFE.PARSE_DATE('%F', SUBSTR(client_info.first_run_date, 1, 10)) AS first_run_date,
  FROM
    `{{ project_id }}.{{ dau_reporting_stable_table }}`
  WHERE
    {% raw %}
    {% if is_init() %}
    DATE(submission_timestamp) >= '2024-10-10'
    {% else %}
    DATE(submission_timestamp) = @submission_date
    {% endif %}
    {% endraw %}
    AND metrics.uuid.usage_profile_id IS NOT NULL
),
-- We need to union with the old dau_reporting ping here as we want to make sure we include data
-- from clients that take longer to update.
reporting_pings_union AS (
  SELECT
    *,
    "usage_reporting" AS source_ping_name,
  FROM usage_reporting_base
  UNION ALL
  SELECT
    *,
    "dau_reporting" AS source_ping_name,
  FROM dau_reporting_base
)
SELECT
  submission_date,
  usage_profile_id,
  udf.mode_last(ARRAY_AGG(normalized_channel IGNORE NULLS ORDER BY submission_timestamp ASC)) AS normalized_channel,
  udf.mode_last(ARRAY_AGG(normalized_country_code IGNORE NULLS ORDER BY submission_timestamp ASC)) AS normalized_country_code,
  udf.mode_last(ARRAY_AGG(normalized_os IGNORE NULLS ORDER BY submission_timestamp ASC)) AS normalized_os,
  udf.mode_last(ARRAY_AGG(normalized_os_version IGNORE NULLS ORDER BY submission_timestamp ASC)) AS normalized_os_version,
  udf.mode_last(ARRAY_AGG(distribution_id IGNORE NULLS ORDER BY submission_timestamp ASC)) AS distribution_id,
  {% if "_desktop" in app_name %}
  COALESCE(LOGICAL_OR(is_active), SUM(browser_engagement_uri_count) > 0 AND SUM(browser_engagement_active_ticks) > 0, FALSE) AS is_active,
  {% else %}
  -- At the moment we do not have duration, default to True.
  -- Eventually is_active value will come from the client.
  COALESCE(LOGICAL_OR(is_active), TRUE) AS is_active,
  {% endif %}
  STRUCT(
    ARRAY_AGG(DISTINCT source_ping_name) AS source_pings
  ),
FROM
  reporting_pings_union
GROUP BY
  submission_date,
  usage_profile_id
