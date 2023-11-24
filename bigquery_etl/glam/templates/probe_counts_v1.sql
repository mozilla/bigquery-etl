{{ header }}
{% if not is_scalar %}

CREATE TEMP FUNCTION udf_get_buckets(
  metric_type STRING,
  range_min INT64,
  range_max INT64,
  bucket_count INT64
)
RETURNS ARRAY<INT64> AS (
  (
    WITH buckets AS (
      SELECT
        CASE
          WHEN metric_type = 'timing_distribution'
            -- https://mozilla.github.io/glean/book/user/metrics/timing_distribution.html
            THEN mozfun.glam.histogram_generate_functional_buckets(2, 8, range_max)
          WHEN metric_type = 'memory_distribution'
            -- https://mozilla.github.io/glean/book/user/metrics/memory_distribution.html
            THEN mozfun.glam.histogram_generate_functional_buckets(2, 16, range_max)
          WHEN metric_type = 'custom_distribution_exponential'
            THEN mozfun.glam.histogram_generate_exponential_buckets(range_min, range_max, bucket_count)
          WHEN metric_type = 'custom_distribution_linear'
            THEN mozfun.glam.histogram_generate_linear_buckets(range_min, range_max, bucket_count)
          ELSE []
        END AS arr
    )
    SELECT
      ARRAY_AGG(CAST(item AS INT64))
    FROM
      buckets
    CROSS JOIN
      UNNEST(arr) AS item
  )
);
{% endif %}

SELECT
    {{ attributes }},
    {{ aggregate_attributes }},
    {% if is_scalar %}
        client_agg_type,
        agg_type,
        {%if channel == "release" %}
          -- Logic to count clients based on sampled windows release data, which started in v119.
          -- If you're changing this, then you'll also need to change
          -- clients_daily_[scalar | histogram]_aggregates
          IF(os = 'Windows' AND app_version >= 119,
            SUM(count) * 10,
            SUM(count)
          ) AS total_users,
        {% else %}
          SUM(count) AS total_users,
        {% endif %}
        mozfun.glam.histogram_fill_buckets_dirichlet(
            mozfun.map.sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
            CASE
              WHEN metric_type IN ({{ scalar_metric_types }})
                THEN ARRAY(SELECT FORMAT("%.*f", 2, bucket) FROM UNNEST(
                    mozfun.glam.histogram_generate_scalar_buckets(range_min, range_max, bucket_count)
                  ) AS bucket ORDER BY bucket)
              WHEN metric_type in ({{ boolean_metric_types }})
                THEN ['always', 'never', 'sometimes']
            END,
            SUM(count)
        )
        AS aggregates
    {% else %}
        agg_type AS client_agg_type,
        'histogram' as agg_type,
        {% if channel == "release" %}
          -- Logic to count clients based on sampled windows release data, which started in v119.
          -- If you're changing this, then you'll also need to change
          -- clients_daily_[scalar | histogram]_aggregates
          IF(os = 'Windows' AND app_version >= 119,
            CAST(ROUND(SUM(record.value)) AS INT64) * 10,
            CAST(ROUND(SUM(record.value)) AS INT64)
          ) AS total_users,
        {% else %}
          CAST(ROUND(SUM(record.value)) AS INT64) AS total_users,
        {% endif %}
        mozfun.glam.histogram_fill_buckets_dirichlet(
            mozfun.map.sum(ARRAY_AGG(record)),
            mozfun.glam.histogram_buckets_cast_string_array(
                udf_get_buckets(metric_type, range_min, range_max, bucket_count)
            ),
            CAST(ROUND(SUM(record.value)) AS INT64)
        ) AS aggregates
    {% endif %}
FROM
    {{ source_table }}
GROUP BY
    {{ attributes }},
    range_min,
    range_max,
    bucket_count,
    {{ aggregate_attributes }},
    {{ aggregate_grouping }}
