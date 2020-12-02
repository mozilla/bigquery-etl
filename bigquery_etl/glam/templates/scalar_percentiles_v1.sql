{{ header }}
{% from 'macros.sql' import enumerate_table_combinations %}

WITH flat_clients_scalar_aggregates AS (
  SELECT
    * EXCEPT (scalar_aggregates)
  FROM
    {{ source_table }}
  CROSS JOIN
    UNNEST(scalar_aggregates)
),
{{
    enumerate_table_combinations(
        "flat_clients_scalar_aggregates",
        "all_combos",
        cubed_attributes,
        attribute_combinations
    )
}},
percentiles AS (
    SELECT
        {{ attributes | join(",") }},
        {{ aggregate_attributes }},
        agg_type AS client_agg_type,
        'percentiles' AS agg_type,
        COUNT(DISTINCT(client_id)) AS total_users,
        APPROX_QUANTILES(value, 100) AS aggregates
    FROM
        all_combos
    GROUP BY
        {{ attributes | join(",") }},
        {{ aggregate_attributes }},
        client_agg_type
)
SELECT
  * REPLACE (mozfun.glam.map_from_array_offsets([5.0, 25.0, 50.0, 75.0, 95.0], aggregates) AS aggregates)
FROM
  percentiles
