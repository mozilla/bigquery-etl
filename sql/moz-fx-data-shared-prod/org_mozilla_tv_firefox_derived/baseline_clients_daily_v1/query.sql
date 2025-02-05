-- Generated via bigquery_etl.glean_usage
WITH base AS (
  SELECT
    submission_timestamp,
    DATE(submission_timestamp) AS submission_date,
    LOWER(client_info.client_id) AS client_id,
    sample_id,
    SAFE.PARSE_DATE('%F', SUBSTR(client_info.first_run_date, 1, 10)) AS first_run_date,
    mozfun.glean.parse_datetime(ping_info.end_time) AS parsed_end_time,
    `moz-fx-data-shared-prod.udf.glean_timespan_seconds`(
      metrics.timespan.glean_baseline_duration
    ) AS duration,
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
    metadata.geo.subdivision1 AS geo_subdivision,
    CAST(NULL AS STRING) AS profile_group_id,
    CAST(NULL AS INT64) AS windows_build_number,
    CAST(NULL AS INT64) AS browser_engagement_uri_count,
    CAST(NULL AS INT64) AS browser_engagement_active_ticks,
    CAST(NULL AS STRING) AS legacy_telemetry_client_id,
    CAST(NULL AS STRING) AS distribution_id,
    CAST(NULL AS BOOLEAN) AS is_default_browser,
    CAST(NULL AS STRING) AS install_source,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_tv_firefox_stable.baseline_v1`
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
overactive AS (
  -- Find client_ids with over 150 000 pings in a day,
  -- which could cause errors in the next step due to aggregation overflows.
  SELECT
    submission_date,
    client_id
  FROM
    with_date_offsets
  WHERE
    {% if is_init() %}
      submission_date >= '2018-01-01'
    {% else %}
      submission_date = @submission_date
    {% endif %}
  GROUP BY
    submission_date,
    client_id
  HAVING
    COUNT(*) > 150000
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
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(normalized_channel) OVER w1
    ) AS normalized_channel,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(normalized_os) OVER w1) AS normalized_os,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(normalized_os_version) OVER w1
    ) AS normalized_os_version,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(android_sdk_version) OVER w1
    ) AS android_sdk_version,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(locale) OVER w1) AS locale,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(city) OVER w1) AS city,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(country) OVER w1) AS country,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(isp) OVER w1) AS isp,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(app_build) OVER w1) AS app_build,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(app_channel) OVER w1) AS app_channel,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(app_display_version) OVER w1
    ) AS app_display_version,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(architecture) OVER w1) AS architecture,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(device_manufacturer) OVER w1
    ) AS device_manufacturer,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(device_model) OVER w1) AS device_model,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(telemetry_sdk_build) OVER w1
    ) AS telemetry_sdk_build,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(distribution_id) OVER w1) AS distribution_id,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(install_source) OVER w1) AS install_source,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(geo_subdivision) OVER w1) AS geo_subdivision,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(profile_group_id) OVER w1
    ) AS profile_group_id,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(windows_build_number) OVER w1
    ) AS windows_build_number,
    SUM(COALESCE(browser_engagement_uri_count, 0)) OVER w1 AS browser_engagement_uri_count,
    SUM(COALESCE(browser_engagement_active_ticks, 0)) OVER w1 AS browser_engagement_active_ticks,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(legacy_telemetry_client_id) OVER w1
    ) AS legacy_telemetry_client_id,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(is_default_browser) OVER w1
    ) AS is_default_browser,
  FROM
    with_date_offsets
  LEFT JOIN
    overactive
    USING (submission_date, client_id)
  WHERE
    overactive.client_id IS NULL
    AND
    {% if is_init() %}
      submission_date >= '2018-01-01'
    {% else %}
      submission_date = @submission_date
    {% endif %}
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
    `moz-fx-data-shared-prod.org_mozilla_tv_firefox_derived.baseline_clients_first_seen_v1` AS cfs
    USING (client_id)
  WHERE
    _n = 1
)
--
SELECT
  *
FROM
  joined
