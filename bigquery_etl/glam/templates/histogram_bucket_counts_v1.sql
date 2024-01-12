{{ header }}
{% from 'macros.sql' import enumerate_table_combinations %}

WITH
{{
    enumerate_table_combinations(
        source_table,
        "all_combos",
        cubed_attributes,
        attribute_combinations
    )
}},
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
      COUNT(DISTINCT client_id) > {{ minimum_client_count }}),
normalized_histograms AS (
  SELECT
    {{ attributes }},
    ARRAY(
      SELECT AS STRUCT
        {{metric_attributes}},
        mozfun.glam.histogram_normalized_sum(value, 1.0) AS aggregates
      FROM unnest(histogram_aggregates)
    )AS histogram_aggregates
  FROM
    all_combos
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
  INNER JOIN build_ids
  USING (app_build_id,channel)
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
    USING (metric_type, metric)