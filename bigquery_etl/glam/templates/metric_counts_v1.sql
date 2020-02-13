{{ header }}
{% if is_scalar %}
    {% include "metric_counts_v1.udf.scalar.sql" %}
{% else %}
    {% include "metric_counts_v1.udf.histogram.sql" %}
{% endif %}

{% for grouping_attributes, hidden_attributes in attribute_combinations %}
    SELECT
        {{ grouping_attributes | join(",") }},
        {% for hidden in hidden_attributes %}
            CAST(NULL AS STRING) AS {{ hidden }},
        {% endfor %}
        {{ aggregate_attributes }},
        {% if is_scalar %}
            client_agg_type,
            agg_type,
            SUM(count) AS total_users,
            CASE
                WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
                THEN udf_fill_buckets(
                udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
                udf_get_buckets()
                )
                WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
                THEN udf_fill_buckets(
                udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
                ['always','never','sometimes'])
            END AS aggregates
        {% else %}
            agg_type AS client_agg_type,
            'histogram' AS agg_type,
            CAST(ROUND(SUM(record.value)) AS INT64) AS total_users,
            udf_fill_buckets(
                udf_dedupe_map_sum(ARRAY_AGG(record)),
                udf_to_string_arr(udf_get_buckets(
                    first_bucket, last_bucket, num_buckets, metric_type
                ))
            ) AS aggregates
            {% endif %}
    FROM
        {{ source_table }}
    WHERE
        first_bucket IS NOT NULL
        {% if "os" in grouping_attributes %}
            AND os IS NOT NULL
        {% endif %}
    GROUP BY
        {{ grouping_attributes | join(",") }},
        {{ aggregate_attributes }},
        {{ aggregate_grouping }}
    {% if not loop.last %}
        UNION ALL
    {% endif %}
{% endfor %}
