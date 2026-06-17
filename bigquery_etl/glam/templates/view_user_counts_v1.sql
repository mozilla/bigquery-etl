{{ header }} {% from 'macros.sql' import generate_static_combos %}
CREATE OR REPLACE VIEW
  `{{ project }}.{{ dataset }}.{{ prefix }}__view_user_counts_v1`
AS
WITH all_clients AS (
  SELECT
    client_id,
    {{ attributes }}
  FROM
    `{{ project }}.{{ dataset }}.{{ prefix }}__clients_scalar_aggregates_v1`
  UNION ALL
  SELECT
    client_id,
    {{ attributes }}
  FROM
    `{{ project }}.{{ dataset }}.{{ prefix }}__clients_histogram_aggregates_v1`
),
-- Pre-aggregate client IDs into HLL sketches before cross joining with
-- attribute combinations to reduce shuffle operations
pre_aggregated AS (
  SELECT
    {{ attributes }},
    HLL_COUNT.INIT(client_id, 24) AS clients_hll
  FROM
    all_clients
  GROUP BY
    {{ attributes }}
),
{{ generate_static_combos(cubed_attributes, attribute_combinations) }},
with_combos AS (
  SELECT
    * EXCEPT ({{ cubed_attributes | join(",") }}),
    {% for attribute in cubed_attributes %}
      COALESCE(
        combo.{{ attribute }},
        t.{{ attribute }}
      ) AS {{ attribute }} {{ "," if not loop.last }}
    {% endfor %}
  FROM
    pre_aggregated AS t
  CROSS JOIN
    static_combos AS combo
)
SELECT
  {{ attributes }},
  HLL_COUNT.MERGE(clients_hll) AS total_users
FROM
  with_combos
GROUP BY
  {{ attributes }}
