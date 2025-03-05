{{ header }}
{% from 'macros.sql' import enumerate_table_combinations, filtered_data, histograms_cte_select %}
WITH
{{
    filtered_data(
        source_table,
        add_windows_release_sample = channel == "release",
        use_sample_id = use_sample_id
    )
}},
build_ids AS (
  SELECT
    app_build_id,
    channel,
  FROM
    filtered_data
  GROUP BY
    1,
    2
  HAVING
    {% if use_sample_id %}
      -- At least <min_client_count>*sample_size clients in the sample
      COUNT(DISTINCT client_id) > {{ minimum_client_count }} * (@max_sample_id - @min_sample_id + 1)/100
    {% else %}
      COUNT(DISTINCT client_id) > {{ minimum_client_count }}
    {% endif %}
),
data_with_enough_clients AS (
  SELECT
    *
  FROM
    filtered_data table
  INNER JOIN
    build_ids
  USING (app_build_id, channel)
),
histograms_cte AS (
    {% set combinations = [
      '
      COALESCE(CAST(NULL AS STRING), ping_type) AS ping_type,
      COALESCE(CAST(NULL AS STRING), os) AS os,
      COALESCE(CAST(NULL AS STRING), app_build_id) AS app_build_id,
      ',
      '
      COALESCE(CAST(NULL AS STRING), ping_type) AS ping_type,
      COALESCE(CAST(NULL AS STRING), os) AS os,
      "*" AS app_build_id,
      ',
      '
      COALESCE(CAST(NULL AS STRING), ping_type) AS ping_type,
      "*" AS os,
      COALESCE(CAST(NULL AS STRING), app_build_id) AS app_build_id,
      ',
      '
      COALESCE(CAST(NULL AS STRING), ping_type) AS ping_type,
      "*" AS os,
      "*" AS app_build_id,
      ',
      '
      "*" AS ping_type,
      COALESCE(CAST(NULL AS STRING), os) AS os,
      COALESCE(CAST(NULL AS STRING), app_build_id) AS app_build_id,
      ',
      '
      "*" AS ping_type,
      COALESCE(CAST(NULL AS STRING), os) AS os,
      "*" AS app_build_id,
      ',
      '
      "*" AS ping_type,
      "*" AS os,
      COALESCE(CAST(NULL AS STRING), app_build_id) AS app_build_id,
      ',
      '
      "*" AS ping_type,
      "*" AS os,
      "*" AS app_build_id,
      '
    ] %}
    {% for combination in combinations %}
      {{
        histograms_cte_select(
          "data_with_enough_clients",
          fixed_attributes,
          metric_attributes,
          channel == "release",
          combination)
      }}
      {% if not loop.last %}
        UNION ALL
      {% endif %}
    {% endfor %}
),
unnested AS (
  SELECT
    {{ attributes }},
    {% for metric_attribute in metric_attributes_list %}
        histogram_aggregates.{{ metric_attribute }} AS {{ metric_attribute }},
    {% endfor %}
    aggregates.key AS bucket,
    aggregates.value AS value,
    aggregates.non_norm_value AS non_norm_value
  FROM
    histograms_cte,
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
                    "{{ meta.type }}" as metric_type,
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
      NOT ENDS_WITH(metric_type, "custom_distribution")
    GROUP BY
      metric_type,
      metric
),
records as (
    SELECT
        {{ attributes }},
        {{ metric_attributes }},
        STRUCT<key STRING, value FLOAT64>(CAST(bucket AS STRING), 1.0 * SUM(value)) AS record,
        STRUCT<key STRING, value FLOAT64>(CAST(bucket AS STRING), 1.0 * SUM(non_norm_value)) AS non_norm_record
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