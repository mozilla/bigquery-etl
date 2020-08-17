WITH all_experiments_searches AS (
  SELECT
    TIMESTAMP_MICROS(CAST(timestamp / 1000 AS INT64)) AS timestamp,
    unnested_experiments,
    unnested_ad_clicks,
    unnested_search_with_ads,
    unnested_search_counts
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.main_summary_v4`,
    UNNEST(experiments) AS unnested_experiments,
    UNNEST(scalar_parent_browser_search_ad_clicks) AS unnested_ad_clicks,
    UNNEST(scalar_parent_browser_search_with_ads) AS unnested_search_with_ads,
    UNNEST(search_counts) AS unnested_search_counts
  WHERE
    submission_date = @submission_date
    AND ARRAY_LENGTH(experiments) > 0
)
SELECT
  unnested_experiments.key AS experiment,
  unnested_experiments.value AS branch,
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(`timestamp`, HOUR),
    -- Aggregates event counts over 5-minute intervals
    INTERVAL(DIV(EXTRACT(MINUTE FROM `timestamp`), 5) * 5) MINUTE
  ) AS window_start,
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(`timestamp`, HOUR),
    INTERVAL((DIV(EXTRACT(MINUTE FROM `timestamp`), 5) + 1) * 5) MINUTE
  ) AS window_end,
  SUM(unnested_ad_clicks.value) AS ad_clicks_count,
  SUM(unnested_search_with_ads.value) AS search_with_ads_count,
  SUM(unnested_search_counts.count) AS search_counts
FROM
  all_experiments_searches
GROUP BY
  experiment,
  branch,
  window_start,
  window_end
