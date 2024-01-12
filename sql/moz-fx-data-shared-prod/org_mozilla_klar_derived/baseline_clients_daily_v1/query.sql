-- Generated via bigquery_etl.glean_usage
WITH base AS (
  SELECT
    submission_timestamp,
    DATE(submission_timestamp) AS submission_date,
    LOWER(client_info.client_id) AS client_id,
    sample_id,
    SAFE.PARSE_DATE('%F', SUBSTR(client_info.first_run_date, 1, 10)) AS first_run_date,
    mozfun.glean.parse_datetime(ping_info.end_time) AS parsed_end_time,
    udf.glean_timespan_seconds(metrics.timespan.glean_baseline_duration) AS duration,
    client_info.android_sdk_version,
    client_info.app_build,
    client_info.app_channel,
    client_info.app_display_version,
    client_info.architecture,
    client_info.device_manufacturer,
    client_info.device_model,
    client_info.telemetry_sdk_build,
    COALESCE(client_info.locale, metrics.string.glean_baseline_locale) AS locale,
    metadata.geo.city,
    metadata.geo.country,
    metadata.isp.name AS isp,
    normalized_channel,
    normalized_os,
    normalized_os_version,
  FROM
    `org_mozilla_klar_stable.baseline_v1`
  -- Baseline pings with 'foreground' reason were first introduced in early April 2020;
  -- we initially excluded them from baseline_clients_daily so that we could measure
  -- effects on KPIs. On 2020-08-25, we removed the filter on reason and backfilled. See:
  -- https://bugzilla.mozilla.org/show_bug.cgi?id=1627286
  -- https://jira.mozilla.com/browse/DS-1018
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
    -- Sums over distinct baseline pings.
    SUM(IF(duration BETWEEN 0 AND 100000, duration, 0)) OVER w1 AS durations,
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
    udf.mode_last(ARRAY_AGG(android_sdk_version) OVER w1) AS android_sdk_version,
    udf.mode_last(ARRAY_AGG(locale) OVER w1) AS locale,
    udf.mode_last(ARRAY_AGG(city) OVER w1) AS city,
    udf.mode_last(ARRAY_AGG(country) OVER w1) AS country,
    udf.mode_last(ARRAY_AGG(isp) OVER w1) AS isp,
    udf.mode_last(ARRAY_AGG(app_build) OVER w1) AS app_build,
    udf.mode_last(ARRAY_AGG(app_channel) OVER w1) AS app_channel,
    udf.mode_last(ARRAY_AGG(app_display_version) OVER w1) AS app_display_version,
    udf.mode_last(ARRAY_AGG(architecture) OVER w1) AS architecture,
    udf.mode_last(ARRAY_AGG(device_manufacturer) OVER w1) AS device_manufacturer,
    udf.mode_last(ARRAY_AGG(device_model) OVER w1) AS device_model,
    udf.mode_last(ARRAY_AGG(telemetry_sdk_build) OVER w1) AS telemetry_sdk_build,
  FROM
    with_date_offsets
  WHERE
    submission_date = @submission_date
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
joined AS (
  SELECT
    cd.* EXCEPT (_n),
    cfs.first_seen_date,
    -- the first seen date may be earlier than the submission date since it also
    -- takes into account the migration ping
    (cd.submission_date = cfs.first_seen_date) AS is_new_profile
  FROM
    windowed AS cd
  LEFT JOIN
    `org_mozilla_klar_derived.baseline_clients_first_seen_v1` AS cfs
    USING (client_id)
  WHERE
    _n = 1
)
--
SELECT
  *
FROM
  joined
