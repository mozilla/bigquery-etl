{{ header }}

{% from 'macros.sql' import enumerate_table_combinations %}

CREATE OR REPLACE VIEW
  `{{ project }}.{{ dataset }}.{{ prefix }}__view_sample_counts_v1`
AS

WITH histogram_data AS (
  SELECT
    client_id,
    {{ attributes }}, 
    h1.metric,
    h1.key,
    h1.agg_type,
    h1.value
  FROM
    `{{ project }}.{{ dataset }}.{{ prefix }}__clients_histogram_aggregates_v1`, UNNEST(histogram_aggregates) h1
    ),
all_clients AS (SELECT
    client_id,
    {{ attributes }}, 
    s1.metric,
    s1.key,
    s1.agg_type,
    s1.value
  FROM
    `{{ project }}.{{ dataset }}.{{ prefix }}__clients_scalar_aggregates_v1`, UNNEST(scalar_aggregates) s1
    WHERE s1.agg_type in ('count', 'false', 'true')
  UNION ALL
  SELECT
    client_id,
    {{ attributes }}, 
    metric,
    v1.key,
    agg_type,
    v1.value
  FROM
    histogram_data, UNNEST(value) v1
),

{{
    enumerate_table_combinations(
        "all_clients",
        "all_combos",
        cubed_attributes,
        attribute_combinations
    )
}}
SELECT
    {{ attributes }},
    metric, 
    key,
    agg_type,
    SUM(value) as total_sample
FROM
    all_combos
GROUP BY
    {{ attributes }}, 
    metric, 
    key,
    agg_type
