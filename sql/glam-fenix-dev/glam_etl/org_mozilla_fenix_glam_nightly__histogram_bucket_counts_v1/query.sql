-- query for org_mozilla_fenix_glam_nightly__histogram_bucket_counts_v1;
WITH
-- Cross join with the attribute combinations to reduce the query complexity
-- with respect to the number of operations. A table with n rows cross joined
-- with a combination of m attributes will generate a new table with n*m rows.
-- The glob ("*") symbol can be understood as selecting all of values belonging
-- to that group.
static_combos AS (
  SELECT
    combos.*
  FROM
    UNNEST(
      ARRAY<STRUCT<ping_type STRING, os STRING, app_build_id STRING>>[
        (NULL, NULL, NULL),
        (NULL, NULL, "*"),
        (NULL, "*", NULL),
        ("*", NULL, NULL),
        (NULL, "*", "*"),
        ("*", NULL, "*"),
        ("*", "*", NULL),
        ("*", "*", "*")
      ]
    ) AS combos
),
all_combos AS (
  SELECT
    table.* EXCEPT (ping_type, os, app_build_id),
    COALESCE(combo.ping_type, table.ping_type) AS ping_type,
    COALESCE(combo.os, table.os) AS os,
    COALESCE(combo.app_build_id, table.app_build_id) AS app_build_id
  FROM
    glam_etl.org_mozilla_fenix_glam_nightly__clients_histogram_aggregates_v1 table
  CROSS JOIN
    static_combos combo
),
build_ids AS (
  SELECT
    app_build_id,
    channel,
  FROM
    all_combos
  GROUP BY
    1,
    2
  HAVING
    COUNT(DISTINCT client_id) > 800
),
normalized_histograms AS (
  SELECT
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    ARRAY(
      SELECT AS STRUCT
        metric,
        metric_type,
        key,
        agg_type,
        mozfun.glam.histogram_normalized_sum(value, 1.0) AS aggregates
      FROM
        UNNEST(histogram_aggregates)
    ) AS histogram_aggregates
  FROM
    all_combos
),
unnested AS (
  SELECT
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
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
  INNER JOIN
    build_ids
    USING (app_build_id, channel)
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
          "search_terms_group_size_distribution" AS metric,
          1 AS range_min,
          4 AS range_max,
          5 AS bucket_count,
          "linear" AS histogram_type
        ),
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
          "geckoview_per_document_site_origins" AS metric,
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
        ),
        STRUCT(
          "custom_distribution" AS metric_type,
          "js_baseline_compile_percentage" AS metric,
          0 AS range_min,
          100 AS range_max,
          20 AS bucket_count,
          "linear" AS histogram_type
        ),
        STRUCT(
          "custom_distribution" AS metric_type,
          "js_delazification_percentage" AS metric,
          0 AS range_min,
          100 AS range_max,
          20 AS bucket_count,
          "linear" AS histogram_type
        ),
        STRUCT(
          "custom_distribution" AS metric_type,
          "js_execution_percentage" AS metric,
          0 AS range_min,
          100 AS range_max,
          20 AS bucket_count,
          "linear" AS histogram_type
        ),
        STRUCT(
          "custom_distribution" AS metric_type,
          "js_xdr_encode_percentage" AS metric,
          0 AS range_min,
          100 AS range_max,
          20 AS bucket_count,
          "linear" AS histogram_type
        ),
        STRUCT(
          "custom_distribution" AS metric_type,
          "performance_clone_deserialize_items" AS metric,
          1 AS range_min,
          2147483646 AS range_max,
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
    MAX(SAFE_CAST(bucket AS INT64)) AS range_max,
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
  USING (metric_type, metric)
