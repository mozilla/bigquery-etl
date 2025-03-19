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
    CAST(NULL AS STRING) AS distribution_id,
    CAST(NULL AS BOOLEAN) AS is_default_browser,
    -- firefox_desktop specific fields.
    COALESCE(metrics.timespan.usage_duration.value, 0) AS duration,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_beta_stable.usage_reporting_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.uuid.usage_profile_id IS NOT NULL
)
SELECT
  submission_date,
  usage_profile_id,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(first_run_date IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS first_run_date,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(app_channel IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS app_channel,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(normalized_country_code IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS normalized_country_code,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(os IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS os,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(os_version IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS os_version,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(app_build IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS app_build,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(app_display_version IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS app_display_version,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(distribution_id IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS distribution_id,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(is_default_browser IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS is_default_browser,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(reason IGNORE NULLS ORDER BY submission_timestamp ASC)
  ) AS reason,
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
