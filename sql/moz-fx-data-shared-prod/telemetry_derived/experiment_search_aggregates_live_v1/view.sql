CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_live_v1`
AS
WITH all_searches AS (
  SELECT
    dataset_id,
    branch,
    experiment,
    window_start,
    window_end,
    ad_clicks_count,
    search_with_ads_count,
    search_count
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry.experiment_search_aggregates_hourly`
  WHERE
    window_start > (
      SELECT
        MAX(window_end)
      FROM
        `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_v1`
    )
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry.experiment_search_aggregates_recents`
)
SELECT
  *,
  SUM(search_count) OVER previous_rows_window AS cumulative_search_count,
  SUM(search_with_ads_count) OVER previous_rows_window AS cumulative_search_with_ads_count,
  SUM(ad_clicks_count) OVER previous_rows_window AS cumulative_ad_clicks_count
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
