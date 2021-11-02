{{ header }}

{% from 'macros.sql' import enumerate_table_combinations %}

CREATE OR REPLACE VIEW
  `{{ project }}.{{ dataset }}.{{ prefix }}__view_sample_counts_v1`
AS
WITH all_clients AS (
  SELECT
    {{ attributes }}
  FROM `{{ project }}`.{{ dataset }}.{{ prefix }}__clients_histogram_aggregates_v1,
  UNNEST(histogram_aggregates) h1

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
  ping_type,
  os,
  app_version,
  app_build_id,
  channel,
  all_combos.key,
  metric,
  SUM(v1.value) AS total_sample
FROM
  all_combos,
  UNNEST(value) AS v1
GROUP BY
  ping_type,
  os,
  app_version,
  app_build_id,
  channel,
  key,
  metric


