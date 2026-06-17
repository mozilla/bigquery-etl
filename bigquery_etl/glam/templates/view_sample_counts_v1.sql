{{ header }} {% from 'macros.sql' import generate_static_combos %}
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
    {% if channel == 'release' %}
      IF(os = 'Windows', 10, 1) AS sample_mult,
    {% endif %}
    h1.metric,
    h1.key,
    h1.agg_type,
    h1.value
  FROM
    `{{ project }}.{{ dataset }}.{{ prefix }}__clients_histogram_aggregates_v1`,
    UNNEST(histogram_aggregates) h1
),
scalars_histogram_data AS (
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    {% if channel == 'release' %}
      IF(os = 'Windows', 10, 1) AS sample_mult,
    {% endif %}
    s1.metric,
    s1.key,
    agg_type,
    s1.value
  FROM
    `{{ project }}.{{ dataset }}.{{ prefix }}__clients_scalar_aggregates_v1`,
    UNNEST(scalar_aggregates) s1
  UNION ALL
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    {% if channel == 'release' %}
      sample_mult,
    {% endif %}
    metric,
    v1.key,
    agg_type,
    v1.value
  FROM
    histogram_data,
    UNNEST(value) v1
),
-- Pre-aggregate before cross joining with attribute combinations
-- to reduce shuffle operations
pre_aggregated AS (
  SELECT
    {{ attributes }},
    metric,
    key,
    agg_type,
    SUM(value) AS value
    {% if channel == 'release' %},
      MAX(sample_mult) AS sample_mult
    {% endif %}
  FROM
    scalars_histogram_data
  GROUP BY
    {{ attributes }},
    metric,
    key,
    agg_type
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
  metric,
  '' AS key,
  agg_type,
  {% if channel == 'release' %}
    CAST(SUM(value) * MAX(sample_mult) AS BIGNUMERIC) AS total_sample
  {% else %}
    CAST(SUM(value) AS BIGNUMERIC) AS total_sample
  {% endif %}
FROM
  with_combos
WHERE
  agg_type = 'summed_histogram'
GROUP BY
  {{ attributes }},
  metric,
  key,
  agg_type
UNION ALL
SELECT
  {{ attributes }},
  metric,
  key,
  agg_type,
  {% if channel == 'release' %}
    CAST(SUM(value) * MAX(sample_mult) AS BIGNUMERIC) AS total_sample
  {% else %}
    CAST(SUM(value) AS BIGNUMERIC) AS total_sample
  {% endif %}
FROM
  with_combos
WHERE
  agg_type <> 'summed_histogram'
GROUP BY
  {{ attributes }},
  metric,
  key,
  agg_type
