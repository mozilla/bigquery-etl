CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_live_v1`
AS
WITH all_searches AS (
  SELECT
    branch,
    experiment,
    window_start,
    window_end,
    ad_clicks_count,
    search_with_ads_count,
    search_count
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.experiment_search_events_live_v1`
  WHERE
    window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  UNION ALL
  SELECT
    experiment,
    branch,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
    -- Aggregates event counts over 5-minute intervals
      INTERVAL(DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) * 5) MINUTE
    ) AS window_start,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
      INTERVAL((DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) + 1) * 5) MINUTE
    ) AS window_end,
    SUM(
      (
        SELECT
          SUM(CAST(REPLACE(x, '"', '') AS INT64))
        FROM
          UNNEST(JSON_EXTRACT_ARRAY(nested_ad_clicks)) AS x
      )
    ) AS ad_clicks,
    SUM(
      (
        SELECT
          SUM(CAST(REPLACE(x, '"', '') AS INT64))
        FROM
          UNNEST(JSON_EXTRACT_ARRAY(nested_search_with_ads)) AS x
      )
    ) AS search_with_ads,
    SUM(
      (
        SELECT
          SUM(CAST(REPLACE(x, '"', '') AS INT64))
        FROM
          UNNEST(JSON_EXTRACT_ARRAY(nested_search_count)) AS x
      )
    ) AS searches,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_derived.experiment_search_events_live_v1`
  GROUP BY
    branch,
    experiment,
    window_start,
    window_end
  WHERE
    window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  UNION ALL
  SELECT
    experiment,
    branch,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
    -- Aggregates event counts over 5-minute intervals
      INTERVAL(DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) * 5) MINUTE
    ) AS window_start,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
      INTERVAL((DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) + 1) * 5) MINUTE
    ) AS window_end,
    SUM(
      (
        SELECT
          SUM(CAST(REPLACE(x, '"', '') AS INT64))
        FROM
          UNNEST(JSON_EXTRACT_ARRAY(nested_ad_clicks)) AS x
      )
    ) AS ad_clicks,
    SUM(
      (
        SELECT
          SUM(CAST(REPLACE(x, '"', '') AS INT64))
        FROM
          UNNEST(JSON_EXTRACT_ARRAY(nested_search_with_ads)) AS x
      )
    ) AS search_with_ads,
    SUM(
      (
        SELECT
          SUM(CAST(REPLACE(x, '"', '') AS INT64))
        FROM
          UNNEST(JSON_EXTRACT_ARRAY(nested_search_count)) AS x
      )
    ) AS searches,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.experiment_search_events_live_v1`
  GROUP BY
    branch,
    experiment,
    window_start,
    window_end
  WHERE
    window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  UNION ALL
  SELECT
    experiment,
    branch,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
    -- Aggregates event counts over 5-minute intervals
      INTERVAL(DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) * 5) MINUTE
    ) AS window_start,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
      INTERVAL((DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) + 1) * 5) MINUTE
    ) AS window_end,
    SUM(
      (
        SELECT
          SUM(CAST(REPLACE(x, '"', '') AS INT64))
        FROM
          UNNEST(JSON_EXTRACT_ARRAY(nested_ad_clicks)) AS x
      )
    ) AS ad_clicks,
    SUM(
      (
        SELECT
          SUM(CAST(REPLACE(x, '"', '') AS INT64))
        FROM
          UNNEST(JSON_EXTRACT_ARRAY(nested_search_with_ads)) AS x
      )
    ) AS search_with_ads,
    SUM(
      (
        SELECT
          SUM(CAST(REPLACE(x, '"', '') AS INT64))
        FROM
          UNNEST(JSON_EXTRACT_ARRAY(nested_search_count)) AS x
      )
    ) AS searches,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_derived.experiment_search_events_live_v1`
  GROUP BY
    branch,
    experiment,
    window_start,
    window_end
  WHERE
    window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  UNION ALL
  SELECT
    branch,
    experiment,
    window_start,
    window_end,
    ad_clicks_count,
    search_with_ads_count,
    search_count
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
    SUM(ad_clicks_count) AS ad_clicks_count,
    SUM(search_with_ads_count) AS search_with_ads_count,
    SUM(search_count) AS search_count
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
  SUM(search_count) OVER previous_rows_window AS cumulative_search_count,
  SUM(search_with_ads_count) OVER previous_rows_window AS cumulative_search_with_ads_count,
  SUM(ad_clicks_count) OVER previous_rows_window AS cumulative_ad_clicks_count
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
