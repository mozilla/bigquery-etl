{{ header }}

WITH base AS (
  SELECT
    submission_timestamp,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    sample_id,
    normalized_channel,
    client_info.app_display_version,
    client_info.app_build,
    normalized_os,
    normalized_os_version,
    client_info.locale,
    -- metadata.geo.country,
    {% if has_distribution_id %}
    metrics.string.metrics_distribution_id AS distribution_id,
    {% else %}
    CAST(NULL AS STRING) AS distribution_id,
    {% endif %}
    {% if has_profile_group_id %}
    metrics.uuid.legacy_telemetry_profile_group_id AS profile_group_id,
    {% else %}
    CAST(NULL AS STRING) AS profile_group_id,
    {% endif %}
    {% if app_name == "firefox_desktop" %}
    metrics.counter.browser_engagement_uri_count,
    metrics.counter.browser_engagement_active_ticks,
    metrics.uuid.legacy_telemetry_client_id,
    {% endif %}
    SAFE.PARSE_DATE('%F', SUBSTR(client_info.first_run_date, 1, 10)) AS first_run_date,
    mozfun.glean.parse_datetime(ping_info.end_time) AS parsed_end_time,
    -- udf.glean_timespan_seconds(metrics.timespan.glean_baseline_duration) AS duration, -- TODO: duration appears to be missing in the ping?
  FROM
    `{{ project_id }}.{{ dau_reporting_stable_table }}`
),
--
with_dates AS (
  SELECT
    *,
    -- For explanation of session start time calculation, see Glean docs:
    -- https://mozilla.github.io/glean/book/user/pings/baseline.html#contents
    DATE(SAFE.TIMESTAMP_SUB(parsed_end_time, INTERVAL duration SECOND)) AS session_start_date,
    DATE(parsed_end_time) AS session_end_date,
  FROM
    base
),
--
with_date_offsets AS (
  SELECT
    *,
    DATE_DIFF(submission_date, session_start_date, DAY) AS session_start_date_offset,
    DATE_DIFF(submission_date, session_end_date, DAY) AS session_end_date_offset,
  FROM
    with_dates
),
--
windowed AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    ROW_NUMBER() OVER w1_unframed AS _n,
    --
    -- Take the earliest first_run_date if ambiguous.
    MIN(first_run_date) OVER w1 AS first_run_date,
    --
    -- Sums over distinct dau_reporting pings.
    -- SUM(IF(duration BETWEEN 0 AND 100000, duration, 0)) OVER w1 AS durations,
    --
    -- Bit patterns capturing activity dates relative to the submission date.
    BIT_OR(
      1 << IF(session_start_date_offset BETWEEN 0 AND 27, session_start_date_offset, NULL)
    ) OVER w1 AS days_seen_session_start_bits,
    BIT_OR(
      1 << IF(session_end_date_offset BETWEEN 0 AND 27, session_end_date_offset, NULL)
    ) OVER w1 AS days_seen_session_end_bits,
    --
    -- For all other dimensions, we use the mode of observed values in the day.
    udf.mode_last(ARRAY_AGG(normalized_channel) OVER w1) AS normalized_channel,
    udf.mode_last(ARRAY_AGG(normalized_os) OVER w1) AS normalized_os,
    udf.mode_last(ARRAY_AGG(normalized_os_version) OVER w1) AS normalized_os_version,
    udf.mode_last(ARRAY_AGG(locale) OVER w1) AS locale,
    -- udf.mode_last(ARRAY_AGG(country) OVER w1) AS country,
    udf.mode_last(ARRAY_AGG(app_build) OVER w1) AS app_build,
    udf.mode_last(ARRAY_AGG(app_display_version) OVER w1) AS app_display_version,
    udf.mode_last(ARRAY_AGG(distribution_id) OVER w1) AS distribution_id,
    udf.mode_last(ARRAY_AGG(profile_group_id) OVER w1) AS profile_group_id,
    {% if app_name == "firefox_desktop" %}
    SUM(browser_engagement_uri_count) AS browser_engagement_uri_count,
    SUM(browser_engagement_active_ticks) AS browser_engagement_active_ticks,
    udf.mode_last(ARRAY_AGG(legacy_telemetry_client_id) OVER w1) AS legacy_telemetry_client_id,
    {% endif %}
  FROM
    with_date_offsets
  WHERE
    {% raw %}
    {% if is_init() %}
    {% endraw %}
      submission_date >= '2024-10-10'
    {% raw %}
    {% else %}
    {% endraw %}
      submission_date = @submission_date
    {% raw %}
    {% endif %}
    {% endraw %}

  WINDOW
    w1 AS (
      PARTITION BY
        sample_id,
        client_id,
        submission_date
      ORDER BY
        submission_timestamp
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ),
    -- We must provide a modified window for ROW_NUMBER which cannot accept a frame clause.
    w1_unframed AS (
      PARTITION BY
        sample_id,
        client_id,
        submission_date
      ORDER BY
        submission_timestamp
    )
),
joined as (
  SELECT
    cd.* EXCEPT (_n),
    cfs.first_seen_date,
    (cd.submission_date = cfs.first_seen_date) AS is_new_profile
  FROM
    windowed AS cd
  LEFT JOIN
    `{{ project_id }}.{{ dau_reporting_clients_first_seen_table }}` AS cfs
    USING (client_id)
  WHERE
    _n = 1
)
SELECT
  *
FROM
  joined
