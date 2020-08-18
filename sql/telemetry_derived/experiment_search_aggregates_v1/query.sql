WITH all_experiments_searches AS (
  SELECT
    submission_timestamp AS timestamp,
    unnested_experiments,
    unnested_ad_clicks,
    unnested_search_with_ads,
    unnested_search_counts
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.main_v4`,
    UNNEST(
      ARRAY(SELECT AS STRUCT key, value.branch AS value FROM UNNEST(environment.experiments))
    ) AS unnested_experiments,
    UNNEST(payload.processes.parent.keyed_scalars.browser_search_ad_clicks) AS unnested_ad_clicks,
    UNNEST(
      payload.processes.parent.keyed_scalars.browser_search_with_ads
    ) AS unnested_search_with_ads,
    UNNEST(
      ARRAY(
        SELECT AS STRUCT
          SUBSTR(_key, 0, pos - 2) AS engine,
          SUBSTR(_key, pos) AS source,
          udf.extract_histogram_sum(value) AS `count`
        FROM
          UNNEST(payload.keyed_histograms.search_counts),
          UNNEST([REPLACE(key, 'in-content.', 'in-content:')]) AS _key,
          UNNEST([LENGTH(REGEXP_EXTRACT(_key, '.+[.].'))]) AS pos
      )
    ) AS unnested_search_counts
  WHERE
    date(submission_timestamp) = @submission_date
    AND ARRAY_LENGTH(environment.experiments) > 0
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
  SUM(unnested_search_counts.count) AS search_count
FROM
  all_experiments_searches
GROUP BY
  experiment,
  branch,
  window_start,
  window_end
