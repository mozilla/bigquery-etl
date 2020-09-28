CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_base`
AS
WITH live_and_stable AS (
  SELECT
    *,
    'telemetry_stable' AS dataset_id
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.main_v4`
  UNION ALL
  SELECT
    *,
    'telemetry_live' AS dataset_id
  FROM
    `moz-fx-data-shared-prod.telemetry_live.main_v4`
)
SELECT
  date(submission_timestamp) AS submission_date,
  dataset_id,
  unnested_experiments.key AS experiment,
  unnested_experiments.value AS branch,
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(submission_timestamp, HOUR),
    -- Aggregates event counts over 5-minute intervals
    INTERVAL(DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) * 5) MINUTE
  ) AS window_start,
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(submission_timestamp, HOUR),
    INTERVAL((DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) + 1) * 5) MINUTE
  ) AS window_end,
  SUM(unnested_ad_clicks.value) AS ad_clicks_count,
  SUM(unnested_search_with_ads.value) AS search_with_ads_count,
  SUM(unnested_search_counts.count) AS search_count,
FROM
  live_and_stable,
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
        `moz-fx-data-shared-prod`.udf.extract_histogram_sum(value) AS `count`
      FROM
        UNNEST(payload.keyed_histograms.search_counts),
        UNNEST([REPLACE(key, 'in-content.', 'in-content:')]) AS _key,
        UNNEST([LENGTH(REGEXP_EXTRACT(_key, '.+[.].'))]) AS pos
    )
  ) AS unnested_search_counts
GROUP BY
  submission_date,
  dataset_id,
  experiment,
  branch,
  window_start,
  window_end
