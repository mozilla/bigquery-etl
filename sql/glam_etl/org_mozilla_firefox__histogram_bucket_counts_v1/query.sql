-- query for org_mozilla_firefox__histogram_bucket_counts_v1;
CREATE TEMP FUNCTION udf_merged_user_data(aggs ANY TYPE)
RETURNS ARRAY<
  STRUCT<
    latest_version INT64,
    metric STRING,
    metric_type STRING,
    key STRING,
    agg_type STRING,
    value ARRAY<STRUCT<key STRING, value INT64>>
  >
> AS (
  (
    WITH unnested AS (
      SELECT
        *
      FROM
        UNNEST(aggs)
    ),
    aggregated_data AS (
      SELECT AS STRUCT
        latest_version,
        metric,
        metric_type,
        key,
        agg_type,
        `moz-fx-data-shared-prod`.udf.map_sum(ARRAY_CONCAT_AGG(value)) AS value
      FROM
        unnested
      GROUP BY
        latest_version,
        latest_version,
        metric,
        metric_type,
        key,
        agg_type
    )
    SELECT
      ARRAY_AGG((latest_version, metric, metric_type, key, agg_type, value))
    FROM
      aggregated_data
  )
);

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
    glam_etl.org_mozilla_firefox__clients_histogram_aggregates_v1 table
  CROSS JOIN
    static_combos combo
),
-- Ensure there is a single record per client id
deduplicated_combos AS (
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    udf_merged_user_data(ARRAY_CONCAT_AGG(histogram_aggregates)) AS histogram_aggregates
  FROM
    all_combos
  GROUP BY
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel
),
normalized_histograms AS (
  SELECT
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    udf_normalize_histograms(histogram_aggregates) AS histogram_aggregates
  FROM
    deduplicated_combos
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
