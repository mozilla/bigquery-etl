-- query runs once per feature + table
WITH
rollouts_cte AS (
  SELECT
    normandy_slug AS rollout_slug
  FROM
    `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
  WHERE
    '{{ feature }}' IN UNNEST(feature_ids)
    AND is_rollout
    AND IF(
      end_date IS NULL,
      @submission_date > start_date,
      @submission_date BETWEEN start_date AND end_date
    )
  UNION ALL
  -- all clients regardless of enrollment
  SELECT "" AS rollout_slug
),
{% for source_table in source_tables %}
  {% set table_dimensions = [] %}
  {% set dimension_aggregators = [] %}
  {% for dimension in dimensions %}
    {% if dimension.source_table == source_table.name %}
      {{ table_dimensions.append(dimension) or "" }}
      {% if dimension.aggregator not in dimension_aggregators %}
        {{ dimension_aggregators.append(dimension.aggregator) or ""}}
      {% endif %}
    {% endif %}
  {% endfor %}
  source_table_cte_{{loop.index}} AS (
    SELECT
      {{ unique_id }} AS unique_id,
      {% if loop.first %}
      ARRAY_AGG(DISTINCT rollout_slug) AS rollout_slugs,
      {% endif %}
      {% if dimension_aggregators != ["last"] %}
        (
          SELECT AS STRUCT
      {% endif %}
      {% if "last" in dimension_aggregators %}
        ARRAY_AGG(
          STRUCT(
            {% for dimension in table_dimensions %}
              {% if dimension.aggregator == "last" %}
                {{ dimension.source_field }} AS {{ dimension.name }}{{ "," if not loop.last }}
              {% endif %}
            {% endfor %}
          )
          ORDER BY {{source_table.sort_field}} DESC
          LIMIT 1
        )[OFFSET(0)]
        {% if dimension_aggregators != ["last"] %}
          .*,
        {% endif %}
      {% endif %}
      {% for dimension in table_dimensions %}
        {% if dimension.aggregator != "last" %}
          {{ aggregator }}({{ dimension.source_field }}) AS {{ dimension.name }},
        {% endif %}
      {% endfor %}
      {% if dimension_aggregators != ["last"] %}
        )
      {% endif %}
      AS dimensions,
      STRUCT(
        {% for metric in metrics %}
          {% if metric.source_table == source_table.name %}
            {{ metric.ping_aggregator }}(
              {% if metric.label_aggregator %}
                (
                  SELECT {{ metric.label_aggregator }}(value)
                  FROM UNNEST({{metric.source_field}})
                  {% if metric.filter_labels %}
                    WHERE key IN (
                      {% for label in metric.label_in %}
                      '{{ label }}'{{ "," if not loop.last }}
                      {% endfor %}
                    )
                  {% endif %}
                )
              {% else %}
                {{ metric.source_field }}
              {% endif %}
            ) AS {{ metric.name }}{{ "," if not loop.last }}
          {% endif %}
        {% endfor %}
      ) AS metrics,
    FROM
      {{ source_table.sql_name }}
    {% if loop.first %}
      , UNNEST(
        ARRAY_CONCAT(
          COALESCE(
            ARRAY(
              SELECT key
              FROM UNNEST(ping_info.experiments)
            ),
            []
          ),
          -- all clients regardless of enrollment
          [""]
        )
      ) AS rollout_slug
      JOIN rollouts_cte
      USING (rollout_slug)
    {% endif %}
    WHERE
      {{ source_table.date_filter }}
    GROUP BY unique_id
  ),
{% endfor %}
aggregates_cte AS (
  SELECT
    rollout_slug,
    {% if (source_tables|length) > 1 %}
      (
        SELECT AS STRUCT
          {% for source_table in source_tables %}
            source_table_cte_{{loop.index}}.dimensions.*,
          {% endfor %}
      ) AS
    {% endif %}
    dimensions,
    COUNT(*) AS count_unique_ids,
    {% for metric in metrics %}
      {% for aggregator in metric.client_aggregators %}
        COALESCE(CAST({{ aggregator }}(metrics.{{ metric.name }}) AS FLOAT64), 0) AS {{ metric.name }}_{{ aggregator }},
      {% endfor %}
    {% endfor %}
  FROM
{% for source_table in source_tables %}
  {% if loop.first %}
    source_table_cte_{{loop.index}},
    UNNEST(rollout_slugs) AS rollout_slug
  {% else %}
    JOIN
      source_table_cte_{{loop.index}}
    USING
      (unique_id)
  {% endif %}
{% endfor %}
  GROUP BY rollout_slug, dimensions
),
ratios_cte AS (
  SELECT
    *,
    {% for numerator, denominator in ratios %}
      SAFE_DIVIDE({{ numerator }}, {{ denominator }}) AS ratio_{{numerator}}_to_{{denominator}},
    {% endfor %}
  FROM
    aggregates_cte
)
SELECT
  @submission_date AS submission_date,
  rollout_slug,
  -- use json so that all features share a schema
  TO_JSON(dimensions) AS dimensions,
  count_unique_ids,
  metric,
  value,
FROM
  ratios_cte
-- unpivot to create consistent schema for wildcard table selects
UNPIVOT (
  value FOR metric IN
  (
    {% for numerator, denominator in ratios %}
      ratio_{{numerator}}_to_{{denominator}},
    {% endfor %}
    {% for metric in metrics %}
      {% for aggregator in metric.client_aggregators %}
        {{ metric.name }}_{{ aggregator.lower() }}{{ "," if not loop.last }}
      {% endfor %}
    {% endfor %}
  )
)
