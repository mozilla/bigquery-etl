WITH all_searches AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_live_v1`
)
SELECT
  window_start AS `time`,
  experiment,
  branch,
  SUM(search_count) OVER previous_rows_window AS value
FROM
  all_searches
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
