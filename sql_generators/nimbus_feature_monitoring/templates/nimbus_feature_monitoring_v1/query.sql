WITH
rollouts_cte AS (
  SELECT
    normandy_slug AS rollout_slug
  FROM
    `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
  WHERE
    "{{ feature.name }}" IN UNNEST(feature_ids)
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
  source_table_cte_{{loop.index}} AS (
    SELECT
      {{ source_table.analysis_unit_id }} AS analysis_unit_id,
      {% if loop.first %}
      ARRAY_AGG(DISTINCT rollout_slug) AS rollout_slugs,
      {% endif %}
      {% if source_table.dimensions %}
        {% if source_table.non_last_dimensions %}
          (
            SELECT AS STRUCT
        {% endif %}
        {% if source_table.last_dimensions %}
          ARRAY_AGG(
            STRUCT(
              {% for dimension in source_table.last_dimensions %}
                {{ dimension.value_expression }} AS {{ dimension.name }}{{ "," if not loop.last }}
              {% endfor %}
            )
            ORDER BY {{source_table.time_partition_field}} DESC
            LIMIT 1
          )[OFFSET(0)]
          {% if source_table.non_last_dimensions %}
            .*,
          {% endif %}
        {% endif %}
        {% for dimension in source_table.non_last_dimensions %}
          {{ dimension.aggregator }}(
            {{dimension.value_expression}}
          ) AS {{ dimension.name }},
        {% endfor %}
        {% if source_table.non_last_dimensions %}
          )
        {% endif %}
        AS dimensions,
      {% endif %}
      {% for metric in feature.metrics_by_source[source_table.name] %}
        {{ metric.ping_aggregator }}(
          {% if metric.data_type.startswith("labeled_") %}
            (
              SELECT {{ metric.label_aggregator }}(value)
              FROM UNNEST({{metric.field}})
              {% if metric.label_in %}
                WHERE key IN (
                  {% for label in metric.label_in %}
                    "{{ label }}"{{ "," if not loop.last }}
                  {% endfor %}
                )
              {% endif %}
            )
          {% elif metric.data_type == "event" %}
            {% if source_table.type == "event_stream" %}
              event_category = "{{ metric.category }}"
              AND event_name = "{{ metric.event_name }}"
            {% else %}
              (
                SELECT COUNT(*)
                FROM UNNEST({{ metric.field }})
                WHERE category = "{{ metric.category }}"
                  AND name = "{{ metric.event_name }}"
              )
            {% endif %}
          {% else %}
            {{ metric.field }}
          {% endif %}
        ) AS {{ metric.name }}{{ "," if not loop.last }}
      {% endfor %}
    FROM
      `{{ source_table.project }}.{{ source_table.dataset }}.{{ source_table.table_name }}`
    {% if loop.first %}
      , UNNEST(
        ARRAY_CONCAT(
          COALESCE(
            {% if source_table.type == "event_stream" %}
              UNNEST(JSON_KEYS(experiments, 1))
            {% else %}
              ARRAY(
                SELECT key
                FROM UNNEST(ping_info.experiments)
              ),
            {% endif %}
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
      CAST({{ source_table.time_partition_field }} AS DATE) = @submission_date
    GROUP BY analysis_unit_id
  ),
{% endfor %}
all_dimensions_and_metrics_cte AS (
  SELECT
    analysis_unit_id,
    rollout_slug,
    {% if (source_tables|length) > 1 %}
      (
        SELECT AS STRUCT
          {% for source_table in source_tables %}
            {% if source_table.dimensions %}
              source_table_cte_{{loop.index}}.dimensions.*,
            {% endif %}
          {% endfor %}
      ) AS
    {% endif %}
    dimensions,
    {% for source_table in source_tables %}
      {% for metric in feature.metrics_by_source[source_table.name] %}
        {{ metric.name }},
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
      (analysis_unit_id)
  {% endif %}
{% endfor %}
),
aggregates_cte AS (
  SELECT
    rollout_slug,
    dimensions,
    COUNT(*) AS count_analysis_unit_ids,
    {% for source_table in source_tables %}
      {% for metric in feature.metrics_by_source[source_table.name] %}
        {% for aggregator in metric.aggregators %}
          COALESCE(CAST(
            {% if aggregator in ("count_true", "count_false", "count_null") %}
              COUNTIF
            {% else %}
              {{ aggregator }}
            {% endif %}
            (
              {{ metric.name }}
              {% if aggregator == "count_true" %}
                IS TRUE
              {% elif aggregator == "count_false" %}
                IS FALSE
              {% elif aggregator == "count_null" %}
                IS NULL
              {% endif %}
            ) AS FLOAT64), 0) AS {{ metric.name }}_{{ aggregator }},
        {% endfor %}
      {% endfor %}
    {% endfor %}
  FROM
    all_dimensions_and_metrics_cte
  GROUP BY rollout_slug, dimensions
),
ratios_cte AS (
  SELECT
    *,
    {% for numerator, denominator in feature.ratios %}
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
  count_analysis_unit_ids,
  metric,
  value,
FROM
  ratios_cte
-- unpivot to create consistent schema for wildcard table selects
UNPIVOT (
  value FOR metric IN
  (
    {% for numerator, denominator in feature.ratios %}
      ratio_{{numerator}}_to_{{denominator}},
    {% endfor %}
    {% for metric, aggregator in feature.all_metrics() %}
      {{ metric.name }}_{{ aggregator.lower() }}{{ "," if not loop.last }}
    {% endfor %}
  )
)
