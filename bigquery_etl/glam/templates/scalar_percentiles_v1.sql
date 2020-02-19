{{ header }}
{% include "scalar_percentiles_v1.udf.sql" %}

WITH flat_clients_scalar_aggregates AS (
  SELECT
    * EXCEPT (scalar_aggregates)
  FROM
    {{ source_table }}
  CROSS JOIN
    UNNEST(scalar_aggregates)
),
percentiles AS (
    {% for attribute_combo in attribute_combinations %}
        SELECT
            {% for attribute, in_grouping in attribute_combo %}
                {% if in_grouping %}
                    {{ attribute }},
                {% else %}
                    NULL AS {{ attribute }},
                {% endif %}
            {% endfor %}
            {{ aggregate_attributes }},
            agg_type AS client_agg_type,
            'percentiles' AS agg_type,
            COUNT(*) AS total_users,
            APPROX_QUANTILES(value, 100) AS aggregates
        FROM
            flat_clients_scalar_aggregates
        GROUP BY
            {% for attribute, in_grouping in attribute_combo if in_grouping %}
                {{ attribute }},
            {% endfor %}
            {{ aggregate_attributes }},
            client_agg_type
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
)
SELECT
  * REPLACE (udf_get_values([5.0, 25.0, 50.0, 75.0, 95.0], aggregates) AS aggregates)
FROM
  percentiles
