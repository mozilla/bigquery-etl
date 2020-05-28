{{ header }}
{% if is_scalar %}
    {% include "probe_counts_v1.udf.scalar.sql" %}
{% else %}
    {% include "probe_counts_v1.udf.histogram.sql" %}
{% endif %}

SELECT
    {{ attributes }},
    {{ aggregate_attributes }},
    {% if is_scalar %}
        client_agg_type,
        agg_type,
        SUM(count) AS total_users,
        CASE
        WHEN
            metric_type IN ({{ scalar_metric_types }})
        THEN
            udf_fill_buckets(
                udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
                udf_get_buckets()
            )
        WHEN
            metric_type in ({{ boolean_metric_types }})
        THEN
            udf_fill_buckets(
                udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
                ['always', 'never', 'sometimes']
            )
        END
        AS aggregates
    {% else %}
        agg_type AS client_agg_type,
        'histogram' as agg_type,
        CAST(ROUND(SUM(record.value)) AS INT64) AS total_users,
        udf_fill_buckets(
            udf_dedupe_map_sum(ARRAY_AGG(record)),
            udf_to_string_arr(
                udf_get_buckets(metric_type, range_min, range_max, bucket_count)
            )
        ) AS aggregates
    {% endif %}
FROM
    {{ source_table }}
GROUP BY
    {{ attributes }},
    {% if not is_scalar %}
        range_min,
        range_max,
        bucket_count,
    {% endif %}
    {{ aggregate_attributes }},
    {{ aggregate_grouping }}
