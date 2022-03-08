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
    v1.value
  FROM
    histogram_data,
    UNNEST(value) v1
),
<<<<<<< HEAD
=======



>>>>>>> 492749c63 (changes to extract probes counts to include total_sample column)
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
    key, 
    metric,
    SUM(value) as total_sample
FROM
    all_combos
GROUP BY
    {{ attributes }}, 
    metric, 
    key
