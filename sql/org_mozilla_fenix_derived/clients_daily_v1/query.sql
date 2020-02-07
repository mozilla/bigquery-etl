WITH unioned AS (
  SELECT
    submission_timestamp,
    document_id,
    client_info,
    sample_id,
    metadata,
    normalized_channel,
    metrics AS baseline_metrics,
    NULL AS metrics
  FROM
    org_mozilla_fenix_stable.baseline_v1
  UNION ALL
  SELECT
    submission_timestamp,
    document_id,
    client_info,
    sample_id,
    metadata,
    normalized_channel,
    NULL AS baseline_metrics,
    metrics
  FROM
    org_mozilla_fenix_stable.metrics_v1
),
  --
base AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    LOWER(client_info.client_id) AS client_id,
    *
  FROM
    unioned
  WHERE
    client_info.client_id IS NOT NULL
),
  --
windowed AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    ROW_NUMBER() OVER w1_unframed AS _n,
    'Fenix' AS app_name,
    --
    -- Take the earliest first_run_date if ambiguous.
    MIN(SAFE.PARSE_DATE('%F', SUBSTR(client_info.first_run_date, 1, 10))) OVER w1 AS first_run_date,
    --
    -- Sums over distinct baseline pings.
    SUM(
      udf.glean_timespan_seconds(baseline_metrics.timespan.glean_baseline_duration)
    ) OVER w1 AS durations,
    --
    -- For all other dimensions, we use the mode of observed values in the day.
    udf.mode_last(ARRAY_AGG(client_info.os) OVER w1) AS os,
    udf.mode_last(ARRAY_AGG(client_info.os_version) OVER w1) AS os_version,
    udf.mode_last(ARRAY_AGG(baseline_metrics.string.glean_baseline_locale) OVER w1) AS locale,
    udf.json_mode_last(
      ARRAY_AGG(udf.geo_struct(metadata.geo.country, metadata.geo.city, NULL, NULL)) OVER w1
    ).* EXCEPT (geo_subdivision1, geo_subdivision2),
    udf.mode_last(ARRAY_AGG(client_info.device_manufacturer) OVER w1) AS device_manufacturer,
    udf.mode_last(ARRAY_AGG(client_info.device_model) OVER w1) AS device_model,
    udf.mode_last(ARRAY_AGG(client_info.app_build) OVER w1) AS app_build,
    udf.mode_last(ARRAY_AGG(normalized_channel) OVER w1) AS normalized_channel,
    udf.mode_last(ARRAY_AGG(client_info.architecture) OVER w1) AS architecture,
    udf.mode_last(
      ARRAY_AGG(metrics.string.search_default_engine_name) OVER w1
    ) AS search_default_engine_name,
    udf.mode_last(
      ARRAY_AGG(metrics.string.search_default_engine_name) OVER w1
    ) AS search_default_engine_code,
    udf.mode_last(ARRAY_AGG(metrics.boolean.metrics_default_browser) OVER w1) AS default_browser,
    udf.mode_last(ARRAY_AGG(client_info.app_display_version) OVER w1) AS app_display_version
  FROM
    base
  WHERE
    -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
    (@submission_date IS NULL OR @submission_date = submission_date)
  WINDOW
    w1 AS (
      PARTITION BY
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
        client_id,
        submission_date
      ORDER BY
        submission_timestamp
    )
)
SELECT
  * EXCEPT (_n)
FROM
  windowed
WHERE
  _n = 1
