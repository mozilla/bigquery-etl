{{ header }}

{% from 'macros.sql' import enumerate_table_combinations %}

CREATE OR REPLACE VIEW
  `{{ project }}.{{ dataset }}.{{ prefix }}__view_sample_counts_v1`
AS

WITH histogram_data AS (
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    h1.metric,
    h1.key,
    h1.agg_type,
    h1.value
  FROM
    `{{ project }}.{{ dataset }}.{{ prefix }}__clients_histogram_aggregates_v1`, UNNEST(histogram_aggregates) h1
),
scalars_histogram_data AS (
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    s1.metric,
    s1.key,
    agg_type,
    s1.value
  FROM
    `{{ project }}.{{ dataset }}.{{ prefix }}__clients_scalar_aggregates_v1`, UNNEST(scalar_aggregates) s1

  UNION ALL 

  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    v1.key,
    agg_type,
    v1.value
  FROM
    histogram_data,
    UNNEST(value) v1
),

{{
    enumerate_table_combinations(
        "scalars_histogram_data",
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
