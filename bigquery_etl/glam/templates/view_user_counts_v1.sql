{{ header }}

{% from 'macros.sql' import enumerate_table_combinations %}

CREATE OR REPLACE VIEW
  `{{ project }}.{{ dataset }}.{{ prefix }}__view_user_counts_v1`
AS
WITH all_clients AS (
  SELECT
    client_id,
    {{ attributes }}
  FROM `{{ project }}`.{{ dataset }}.{{ prefix }}__clients_scalar_aggregates_v1
  
  UNION ALL
  
  SELECT
    client_id,
    {{ attributes }}
  FROM `{{ project }}`.{{ dataset }}.{{ prefix }}__clients_histogram_aggregates_v1
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
    COUNT(DISTINCT client_id) as total_users
FROM
    all_combos
GROUP BY
    {{ attributes }}
