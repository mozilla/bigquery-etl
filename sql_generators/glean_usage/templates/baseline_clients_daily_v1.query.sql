{{ header }}

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
    metadata.geo.subdivision1 AS geo_subdivision,
    {% if has_profile_group_id %}
    metrics.uuid.legacy_telemetry_profile_group_id AS profile_group_id,
    {% else %}
    CAST(NULL AS STRING) AS profile_group_id,
    {% endif %}
    {% if app_name == "firefox_desktop" %}
    client_info.windows_build_number AS windows_build_number,
    metrics.counter.browser_engagement_uri_count as browser_engagement_uri_count,
    metrics.counter.browser_engagement_active_ticks as browser_engagement_active_ticks,
    metrics.uuid.legacy_telemetry_client_id as legacy_telemetry_client_id,
    metrics.string.usage_distribution_id AS distribution_id,
    metrics.boolean.usage_is_default_browser AS is_default_browser,
    CAST(NULL AS STRING) AS install_source,
    {% elif app_name == "fenix" %}
    CAST(NULL AS INT64) AS windows_build_number,
    CAST(NULL AS INT64) AS browser_engagement_uri_count,
    CAST(NULL AS INT64) AS browser_engagement_active_ticks,
    CAST(NULL AS STRING) AS legacy_telemetry_client_id,
    metrics.string.metrics_distribution_id AS distribution_id,
    CAST(NULL AS BOOLEAN) AS is_default_browser,
    metrics.string.first_session_install_source AS install_source,
    {% else %}
    CAST(NULL AS INT64) AS windows_build_number,
    CAST(NULL AS INT64) AS browser_engagement_uri_count,
    CAST(NULL AS INT64) AS browser_engagement_active_ticks,
    CAST(NULL AS STRING) AS legacy_telemetry_client_id,
    CAST(NULL AS STRING) AS distribution_id,
    CAST(NULL AS BOOLEAN) AS is_default_browser,
    CAST(NULL AS STRING) AS install_source,
    {% endif %}
    client_info.attribution,
    client_info.distribution,
  FROM
    `{{ baseline_table }}`
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
    {% raw %}
    {% if is_init() %}
    {% endraw %}
      submission_date >= '2018-01-01'
    {% raw %}
    {% else %}
    {% endraw %}
      submission_date = @submission_date
    {% raw %}
    {% endif %}
    {% endraw %}
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
    udf.mode_last(ARRAY_AGG(distribution_id) OVER w1) AS distribution_id,
    udf.mode_last(ARRAY_AGG(install_source) OVER w1) AS install_source,
    udf.mode_last(ARRAY_AGG(geo_subdivision) OVER w1) AS geo_subdivision,
    udf.mode_last(ARRAY_AGG(profile_group_id) OVER w1) AS profile_group_id,
    udf.mode_last(ARRAY_AGG(windows_build_number) OVER w1) AS windows_build_number,
    SUM(COALESCE(browser_engagement_uri_count, 0)) OVER w1 AS browser_engagement_uri_count,
    SUM(COALESCE(browser_engagement_active_ticks, 0)) OVER w1 AS browser_engagement_active_ticks,
    udf.mode_last(ARRAY_AGG(legacy_telemetry_client_id) OVER w1) AS legacy_telemetry_client_id,
    udf.mode_last(ARRAY_AGG(is_default_browser) OVER w1) AS is_default_browser,
    udf.mode_last(ARRAY_AGG(attribution) OVER w1) AS attribution,
    udf.mode_last(ARRAY_AGG(`distribution`) OVER w1) AS `distribution`,
  FROM
    with_date_offsets
  LEFT JOIN
    overactive
    USING (submission_date, client_id)
  WHERE
    overactive.client_id IS NULL AND
    {% raw %}
    {% if is_init() %}
    {% endraw %}
      submission_date >= '2018-01-01'
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
    -- the first seen date may be earlier than the submission date since it also
    -- takes into account the migration ping
    (cd.submission_date = cfs.first_seen_date) AS is_new_profile
  FROM
    windowed AS cd
  LEFT JOIN
    `{{ first_seen_table }}` AS cfs
    USING (client_id)
  WHERE
    _n = 1
)
--
SELECT
  *
FROM
  joined
