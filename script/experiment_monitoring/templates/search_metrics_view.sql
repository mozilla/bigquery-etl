-- Generated via bigquery-etl script/experiment_monitoring/generate_search_metrics_views.py
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_live_v1`
AS
WITH all_searches AS (
  { % FOR dataset IN datasets - % }
  SELECT
    branch,
    experiment,
    window_start,
    window_end,
    { % FOR metric IN metrics - % } { {metric } },
    { %endfor - % }
  FROM
    { % FOR metric IN metrics - % } { %
    IF
      LOOP
        .index != 1 - % }
        FULL OUTER JOIN
          `moz-fx-data-shared-prod.{{ dataset }}.experiment_{{ metric }}_live_v1`
        USING
          (branch, experiment, window_start, window_end) { %
      ELSE
        - % }`moz-fx-data-shared-prod.{{ dataset }}.experiment_{{ metric }}_live_v1` { %endif - % } { %endfor - % }
        WHERE
          window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
        UNION ALL
          { %endfor - % }
        SELECT
          branch,
          experiment,
          window_start,
          window_end,
          { % FOR metric IN metrics - % } { {metric } },
          { %endfor - % }
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
    { % FOR metric IN metrics - % }SUM({ {metric } })
  AS
    { {metric } },
    { %endfor - % }
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
  { % FOR metric IN metrics - % }SUM(
    { {metric } }
  ) OVER previous_rows_window AS cumulative_ { {metric } },
  { %endfor - % }
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
