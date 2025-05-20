-- Generated via `usage_reporting` SQL generator.
WITH usage_reporting_base AS (
  SELECT
    submission_timestamp,
    DATE(submission_timestamp) AS submission_date,
    metrics.uuid.usage_profile_id,
    SAFE.PARSE_DATE('%F', SUBSTR(metrics.datetime.usage_first_run_date, 1, 10)) AS first_run_date,
    metrics.string.usage_app_channel AS app_channel,
    normalized_country_code,
    metrics.string.usage_os AS os,
    metrics.string.usage_os_version AS os_version,
    metrics.string.usage_app_display_version AS app_display_version,
    metrics.string.usage_app_build AS app_build,
    metrics.string.usage_reason AS reason,
    CAST(NULL AS BOOLEAN) AS is_active,  -- Eventually is_active value will come from the client.
    -- fields only currently available in fenix.
    metrics.string.metrics_distribution_id AS distribution_id,
    COALESCE(
      metrics.boolean.metrics_default_browser,
      FALSE
    ) AS is_default_browser, -- this should eventually also be added to ios
    -- firefox_desktop specific fields.
    COALESCE(metrics.timespan.usage_duration.value, 0) AS duration,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.usage_reporting_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.uuid.usage_profile_id IS NOT NULL
)
SELECT
  submission_date,
  usage_profile_id,
  udf.mode_last(
    ARRAY_AGG(first_run_date IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS first_run_date,
  udf.mode_last(
    ARRAY_AGG(app_channel IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS app_channel,
  udf.mode_last(
    ARRAY_AGG(normalized_country_code IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS normalized_country_code,
  udf.mode_last(ARRAY_AGG(os IGNORE NULLS ORDER BY submission_timestamp ASC)) AS os,
  udf.mode_last(ARRAY_AGG(os_version IGNORE NULLS ORDER BY submission_timestamp ASC)) AS os_version,
  udf.mode_last(ARRAY_AGG(app_build IGNORE NULLS ORDER BY submission_timestamp ASC)) AS app_build,
  udf.mode_last(
    ARRAY_AGG(app_display_version IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS app_display_version,
  udf.mode_last(
    ARRAY_AGG(distribution_id IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS distribution_id,
  udf.mode_last(
    ARRAY_AGG(is_default_browser IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS is_default_browser,
  udf.mode_last(ARRAY_AGG(reason IGNORE NULLS ORDER BY submission_timestamp ASC)) AS reason,
  -- is_active definition is different between desktop and mobile products.
  COALESCE(
    LOGICAL_OR(is_active),
    SUM(IF(duration BETWEEN 0 AND 100000, duration, 0)) > 0,
    FALSE
  ) AS is_active,
FROM
  usage_reporting_base
GROUP BY
  submission_date,
  usage_profile_id
