CREATE TEMP FUNCTION udf_normalized_sum(arrs ARRAY<STRUCT<key STRING, value INT64>>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Returns the normalized sum of the input maps.
  -- It returns the total_count[k] / SUM(total_count)
  -- for each key k.
  (
    WITH total_counts AS (
      SELECT
        sum(a.value) AS total_count
      FROM
        UNNEST(arrs) AS a
    ),
    summed_counts AS (
      SELECT
        a.key AS k,
        SUM(a.value) AS v
      FROM
        UNNEST(arrs) AS a
      GROUP BY
        a.key
    ),
    final_values AS (
      SELECT
        STRUCT<key STRING, value FLOAT64>(
          k,
          COALESCE(SAFE_DIVIDE(1.0 * v, total_count), 0)
        ) AS record
      FROM
        summed_counts
      CROSS JOIN
        total_counts
    )
    SELECT
      ARRAY_AGG(record)
    FROM
      final_values
  )
);

CREATE TEMP FUNCTION udf_normalize_histograms(
  arrs ARRAY<
    STRUCT<
      latest_version INT64,
      metric STRING,
      metric_type STRING,
      key STRING,
      agg_type STRING,
      value ARRAY<STRUCT<key STRING, value INT64>>
    >
  >
)
RETURNS ARRAY<
  STRUCT<
    latest_version INT64,
    metric STRING,
    metric_type STRING,
    key STRING,
    agg_type STRING,
    aggregates ARRAY<STRUCT<key STRING, value FLOAT64>>
  >
> AS (
  (
    WITH normalized AS (
      SELECT
        latest_version,
        metric,
        metric_type,
        key,
        agg_type,
        -- NOTE: dropping the actual sum here, since it isn't being used
        udf_normalized_sum(value) AS aggregates
      FROM
        UNNEST(arrs)
    )
    SELECT
      ARRAY_AGG((latest_version, metric, metric_type, key, agg_type, aggregates))
    FROM
      normalized
  )
);

WITH normalized_histograms AS (
  SELECT
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    udf_normalize_histograms(histogram_aggregates) AS histogram_aggregates
  FROM
    glam_etl.org_mozilla_fenix__clients_histogram_aggregates_v1
),
unnested AS (
  SELECT
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    histogram_aggregates.latest_version AS latest_version,
    histogram_aggregates.metric AS metric,
    histogram_aggregates.metric_type AS metric_type,
    histogram_aggregates.key AS key,
    histogram_aggregates.agg_type AS agg_type,
    aggregates.key AS bucket,
    aggregates.value
  FROM
    normalized_histograms,
    UNNEST(histogram_aggregates) AS histogram_aggregates,
    UNNEST(aggregates) AS aggregates
),
-- Find information that can be used to construct the bucket range. Most of the
-- distributions follow a bucketing rule of 8*log2(n). This doesn't apply to the
-- custom distributions e.g. GeckoView, which needs to incorporate information
-- from the probe info service.
-- See: https://mozilla.github.io/glean/book/user/metrics/custom_distribution.html
distribution_metadata AS (
  SELECT
    *
  FROM
    UNNEST(
      [
        STRUCT(
          "custom_distribution" AS metric_type,
          "geckoview_document_site_origins" AS metric,
          0 AS range_min,
          100 AS range_max,
          50 AS bucket_count,
          "exponential" AS histogram_type
        ),
        STRUCT(
          "custom_distribution" AS metric_type,
          "gfx_checkerboard_peak_pixel_count" AS metric,
          1 AS range_min,
          66355200 AS range_max,
          50 AS bucket_count,
          "exponential" AS histogram_type
        ),
        STRUCT(
          "custom_distribution" AS metric_type,
          "gfx_checkerboard_severity" AS metric,
          1 AS range_min,
          1073741824 AS range_max,
          50 AS bucket_count,
          "exponential" AS histogram_type
        ),
        STRUCT(
          "custom_distribution" AS metric_type,
          "gfx_content_frame_time_from_paint" AS metric,
          1 AS range_min,
          5000 AS range_max,
          50 AS bucket_count,
          "exponential" AS histogram_type
        ),
        STRUCT(
          "custom_distribution" AS metric_type,
          "gfx_content_frame_time_from_vsync" AS metric,
          8 AS range_min,
          792 AS range_max,
          100 AS bucket_count,
          "linear" AS histogram_type
        ),
        STRUCT(
          "custom_distribution" AS metric_type,
          "gfx_content_frame_time_with_svg" AS metric,
          1 AS range_min,
          5000 AS range_max,
          50 AS bucket_count,
          "exponential" AS histogram_type
        ),
        STRUCT(
          "custom_distribution" AS metric_type,
          "gfx_content_frame_time_without_resource_upload" AS metric,
          1 AS range_min,
          5000 AS range_max,
          50 AS bucket_count,
          "exponential" AS histogram_type
        ),
        STRUCT(
          "custom_distribution" AS metric_type,
          "gfx_content_frame_time_without_upload" AS metric,
          1 AS range_min,
          5000 AS range_max,
          50 AS bucket_count,
          "exponential" AS histogram_type
        )
      ]
    )
  UNION ALL
  SELECT
    metric_type,
    metric,
    NULL AS range_min,
    MAX(CAST(bucket AS INT64)) AS range_max,
    NULL AS bucket_count,
    NULL AS histogram_type
  FROM
    unnested
  WHERE
    metric_type <> "custom_distribution"
  GROUP BY
    metric_type,
    metric
),
records AS (
  SELECT
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    latest_version,
    metric,
    metric_type,
    key,
    agg_type,
    STRUCT<key STRING, value FLOAT64>(CAST(bucket AS STRING), 1.0 * SUM(value)) AS record
  FROM
    unnested
  GROUP BY
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    latest_version,
    metric,
    metric_type,
    key,
    agg_type,
    bucket
)
SELECT
  * EXCEPT (metric_type, histogram_type),
    -- Suffix `custom_distribution` with bucketing type
  IF(
    histogram_type IS NOT NULL,
    CONCAT(metric_type, "_", histogram_type),
    metric_type
  ) AS metric_type
FROM
  records
LEFT OUTER JOIN
  distribution_metadata
USING
  (metric_type, metric)
