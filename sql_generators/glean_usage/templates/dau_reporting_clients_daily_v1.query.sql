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
    mozfun.glean.parse_datetime(ping_info.end_time) AS parsed_end_time,
    -- TODO: add duration once it has been added to the dau_reporting ping.
    -- udf.glean_timespan_seconds(metrics.timespan.glean_baseline_duration) AS duration,
  FROM
    `{{ project_id }}.{{ dau_reporting_stable_table }}`
  WHERE
    usage_profile_id IS NOT NULL
),
--
with_dates AS (
  SELECT
    *,
    -- For explanation of session start time calculation, see Glean docs:
    -- https://mozilla.github.io/glean/book/user/pings/baseline.html#contents
    --
    -- TODO: uncomment once duration is added to the dau_reporting ping
    -- DATE(SAFE.TIMESTAMP_SUB(parsed_end_time, INTERVAL duration SECOND)) AS session_start_date,
    DATE(parsed_end_time) AS session_end_date,
  FROM
    base
),
--
with_date_offsets AS (
  SELECT
    *,
    -- TODO: uncomment once duration is added to the dau_reporting ping
    -- DATE_DIFF(submission_date, session_start_date, DAY) AS session_start_date_offset,
    DATE_DIFF(submission_date, session_end_date, DAY) AS session_end_date_offset,
  FROM
    with_dates
),
--
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
  -- SUM(browser_engagement_uri_count) AS browser_engagement_uri_count,
  -- SUM(browser_engagement_active_ticks) AS browser_engagement_active_ticks,
  {% else %}
  -- At the moment we do not have duration, default to True.
  -- TODO: uncomment once duration is added to the dau_reporting ping
  -- COALESCE(is_active, SUM(IF(duration BETWEEN 0 AND 100000, duration, 0)) OVER w1 > 0, False) AS is_active,
  True AS is_active
  {% endif %}
  --
  -- TODO: uncomment once duration is added to the dau_reporting ping
  --
  -- Bit patterns capturing activity dates relative to the submission date.
  -- BIT_OR(
  --   1 << IF(session_start_date_offset BETWEEN 0 AND 27, session_start_date_offset, NULL)
  -- ) OVER w1 AS days_seen_session_start_bits,
  -- BIT_OR(
  --   1 << IF(session_end_date_offset BETWEEN 0 AND 27, session_end_date_offset, NULL)
  -- ) OVER w1 AS days_seen_session_end_bits,
  --
FROM
  with_date_offsets
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
