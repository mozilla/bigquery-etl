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
        {{ metric_attributes }},
        -- NOTE: dropping the actual sum here, since it isn't being used
        udf_normalized_sum(value) AS aggregates
      FROM
        UNNEST(arrs)
    )
    SELECT
      ARRAY_AGG(({{ metric_attributes }}, aggregates))
    FROM
      normalized
  )
);

WITH normalized_histograms AS (
  SELECT
    {{ attributes }},
    udf_normalize_histograms(histogram_aggregates) AS histogram_aggregates
  FROM
    glam_etl.{{ prefix }}__clients_histogram_aggregates_v1
),
unnested AS (
  SELECT
    {{ attributes }},
    {% for metric_attribute in metric_attributes_list %}
        histogram_aggregates.{{ metric_attribute }} AS {{ metric_attribute }},
    {% endfor %}
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
    SELECT *
    FROM UNNEST(
        [
            {% for meta in custom_distribution_metadata_list %}
                STRUCT(
                    "custom_distribution" as metric_type,
                    "{{ meta.name.replace('.', '_') }}" as metric,
                    {{ meta.range_min }} as range_min,
                    {{ meta.range_max }} as range_max,
                    {{ meta.bucket_count }} as bucket_count,
                    "{{ meta.histogram_type }}" as histogram_type
                )
                {{ "," if not loop.last else "" }}
            {% endfor %}
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
records as (
    SELECT
        {{ attributes }},
        {{ metric_attributes }},
        STRUCT<key STRING, value FLOAT64>(CAST(bucket AS STRING), 1.0 * SUM(value)) AS record
    FROM
        unnested
    GROUP BY
        {{ attributes }},
        {{ metric_attributes }},
        bucket
)
SELECT
    * EXCEPT(metric_type, histogram_type),
    -- Suffix `custom_distribution` with bucketing type
    IF(
      histogram_type IS NOT NULL,
      CONCAT(metric_type, "_", histogram_type),
      metric_type
    ) as metric_type
FROM
    records
LEFT OUTER JOIN
    distribution_metadata
USING
    (metric_type, metric)
