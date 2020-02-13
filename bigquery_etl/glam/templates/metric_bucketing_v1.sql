{{ header }} 
{% include "metric_bucketing_v1.udf.sql" %}

{% for grouping_attributes, hidden_attributes in attribute_combinations %}
    SELECT
        {{ grouping_attributes | join(",") }},
        {% for hidden in hidden_attributes %}
            CAST(NULL AS STRING) AS {{ hidden }},
        {% endfor %}
        {{ aggregate_attributes }},
        agg_type AS client_agg_type,
        'histogram' AS agg_type,
        CAST(ROUND(SUM(record.value)) AS INT64) AS total_users,
        udf_fill_buckets(
            udf_dedupe_map_sum(ARRAY_AGG(record)),
            udf_to_string_arr(udf_get_buckets(
                first_bucket, last_bucket, num_buckets, metric_type
            ))
        ) AS aggregates
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
