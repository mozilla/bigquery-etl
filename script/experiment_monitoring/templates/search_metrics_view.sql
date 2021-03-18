-- Generated via bigquery-etl script/experiment_monitoring/generate_search_metrics_views.py
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_live_v1`
AS
WITH all_searches AS (
  {% for dataset in datasets -%}
  {% for metric in metrics -%}
  SELECT
    branch,
    experiment,
    window_start,
    window_end,
    {% for m in metrics -%}
    {% if m == metric -%}
    {{ metric }},
    {% else -%}
    0 AS {{ m }},
    {% endif -%}
    {% endfor -%}
  FROM
    `moz-fx-data-shared-prod.{{ dataset }}.experiment_{{ metric }}_live_v1`
  WHERE
    window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  UNION ALL
  {% endfor -%}
  {% endfor -%}
  SELECT
    branch,
    experiment,
    window_start,
    window_end,
    {% for metric in metrics -%} 
    {{ metric }},
    {% endfor -%}
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_v1`
  WHERE
    window_start <= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
),
grouped_searches AS (
  SELECT
    branch,
    experiment,
    window_start,
    window_end,
    {% for metric in metrics -%}
    SUM({{ metric }}) AS {{ metric }},
    {% endfor -%}
  FROM
    all_searches
  GROUP BY
    branch,
    experiment,
    window_start,
    window_end
)
SELECT
  *,
  {% for metric in metrics -%}
  SUM({{ metric }}) OVER previous_rows_window AS cumulative_{{ metric }},
  {% endfor -%}
FROM
  grouped_searches
WINDOW
  previous_rows_window AS (
    PARTITION BY
      experiment,
      branch
    ORDER BY
      window_start
    ROWS BETWEEN
      UNBOUNDED PRECEDING
      AND CURRENT ROW
  )
