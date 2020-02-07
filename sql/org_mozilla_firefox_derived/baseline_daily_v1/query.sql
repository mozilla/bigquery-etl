/*

This is a daily aggregation of just baseline pings from across the various
apps that correspond to the different channels of "Fenix", the new Firefox for Android.

*/
-- The schemas for the different apps differ slightly such that we can't
-- blindly union over the baseline views; instead, we extract out the set of
-- fields we want via a UDF before we union.
CREATE TEMP FUNCTION extract_fields(baseline ANY TYPE) AS (
  (
    SELECT AS STRUCT
      baseline.submission_timestamp,
      DATE(baseline.submission_timestamp) AS submission_date,
      LOWER(baseline.client_info.client_id) AS client_id,
      baseline.sample_id,
      SAFE.PARSE_DATE('%F', SUBSTR(baseline.client_info.first_run_date, 1, 10)) AS first_run_date,
      baseline.ping_info.parsed_end_time AS end_time,
      udf.glean_timespan_seconds(baseline.metrics.timespan.glean_baseline_duration) AS duration,
      baseline.client_info.android_sdk_version,
      baseline.client_info.app_build,
      baseline.client_info.app_channel,
      baseline.client_info.app_display_version,
      baseline.client_info.architecture,
      baseline.client_info.device_manufacturer,
      baseline.client_info.device_model,
      baseline.client_info.telemetry_sdk_build,
      baseline.client_info.locale,
      baseline.metadata.geo.city,
      baseline.metadata.geo.country,
      baseline.metrics.string.glean_baseline_locale,
      baseline.normalized_os,
      baseline.normalized_os_version,
  )
);

WITH base AS (
  SELECT
    extract_fields(baseline).*,
    'release' AS normalized_channel
  FROM
    org_mozilla_firefox.baseline AS baseline
  UNION ALL
  SELECT
    extract_fields(baseline).*,
    'beta' AS normalized_channel
  FROM
    org_mozilla_firefox_beta.baseline AS baseline
  UNION ALL
  SELECT
    extract_fields(baseline).*,
    'nightly' AS normalized_channel
  FROM
    org_mozilla_fenix.baseline AS baseline
  -- These final two apps should be retired in February.
  UNION ALL
  SELECT
    extract_fields(baseline).*,
    'aurora nightly' AS normalized_channel
  FROM
    org_mozilla_fennec_aurora.baseline AS baseline
  UNION ALL
  SELECT
    extract_fields(baseline).*,
    'preview nightly' AS normalized_channel
  FROM
    org_mozilla_fenix_nightly.baseline AS baseline
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
    -- Maintain lists of unique client activity dates for usage criteria based on client timestamps.
    ARRAY_AGG(DATE(TIMESTAMP_SUB(end_time, INTERVAL duration SECOND))) OVER w1 AS start_dates,
    ARRAY_AGG(DATE(end_time)) OVER w1 AS end_dates,
    --
    -- For all other dimensions, we use the mode of observed values in the day.
    udf.mode_last(ARRAY_AGG(normalized_channel) OVER w1) AS normalized_channel,
    udf.mode_last(ARRAY_AGG(android_sdk_version) OVER w1) AS android_sdk_version,
    udf.mode_last(ARRAY_AGG(app_build) OVER w1) AS app_build,
    udf.mode_last(ARRAY_AGG(app_channel) OVER w1) AS app_channel,
    udf.mode_last(ARRAY_AGG(app_display_version) OVER w1) AS app_display_version,
    udf.mode_last(ARRAY_AGG(architecture) OVER w1) AS architecture,
    udf.mode_last(ARRAY_AGG(device_manufacturer) OVER w1) AS device_manufacturer,
    udf.mode_last(ARRAY_AGG(device_model) OVER w1) AS device_model,
    udf.mode_last(ARRAY_AGG(telemetry_sdk_build) OVER w1) AS telemetry_sdk_build,
    udf.mode_last(ARRAY_AGG(locale) OVER w1) AS locale,
    udf.mode_last(ARRAY_AGG(city) OVER w1) AS city,
    udf.mode_last(ARRAY_AGG(country) OVER w1) AS country,
    udf.mode_last(ARRAY_AGG(glean_baseline_locale) OVER w1) AS glean_baseline_locale,
    udf.mode_last(ARRAY_AGG(normalized_os) OVER w1) AS os,
    udf.mode_last(ARRAY_AGG(normalized_os_version) OVER w1) AS os_version,
  FROM
    base
  WHERE
    -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
    (@submission_date IS NULL OR @submission_date = submission_date)
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
)
--
SELECT
  wnd.submission_date,
  cfs.fenix_first_seen_date,
  cfs.fennec_first_seen_date,
  wnd.* EXCEPT (_n, submission_date) REPLACE(
    ARRAY(SELECT DISTINCT d FROM UNNEST(start_dates) d WHERE d IS NOT NULL) AS start_dates,
    ARRAY(SELECT DISTINCT d FROM UNNEST(end_dates) d WHERE d IS NOT NULL) AS end_dates
  )
FROM
  windowed AS wnd
-- We incur the expense of joining in first_seen dates here so that we can
-- identify returning users cheaply in further views on top of baseline_daily.
LEFT JOIN
  clients_first_seen_v1 AS cfs
USING
  (client_id)
WHERE
  _n = 1
