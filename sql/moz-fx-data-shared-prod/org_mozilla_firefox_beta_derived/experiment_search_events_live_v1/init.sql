-- This and the following materialized view need to be kept in sync:
-- - org_mozilla_fenix_derived.experiment_search_events_live_v1
-- - org_mozilla_firefox_beta_derived.experiment_search_events_live_v1
-- - org_mozilla_firefox_derived.experiment_search_events_live_v1
CREATE MATERIALIZED VIEW
IF
  NOT EXISTS `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.experiment_search_events_live_v1`
  OPTIONS
    (enable_refresh = TRUE, refresh_interval_minutes = 5)
  AS
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    -- Materialized views do not support sub-queries for UNNESTing fields
    -- Extract the counts from the JSON of the nested struct AS single array
    TO_JSON_STRING(
      REGEXP_EXTRACT_ALL(
        TO_JSON_STRING(metrics.labeled_counter.browser_search_ad_clicks),
        '"value":([^,}]+)'
      )
    ) AS ad_clicks,
    TO_JSON_STRING(
      REGEXP_EXTRACT_ALL(
        TO_JSON_STRING(metrics.labeled_counter.browser_search_with_ads),
        '"value":([^,}]+)'
      )
    ) AS search_with_ads,
    TO_JSON_STRING(
      REGEXP_EXTRACT_ALL(
        TO_JSON_STRING(metrics.labeled_counter.metrics_search_count),
        '"value":([^,}]+)'
      )
    ) AS search_count
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_live.metrics_v1`,
    UNNEST(ping_info.experiments) AS experiment
  WHERE
    -- Limit the amount of data the materialized view is going to backfill when created.
    -- This date can be moved forward whenever new changes of the materialized views need to be deployed.
    DATE(submission_timestamp) >= DATE('2021-03-01')
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6

-- Another attempt
-- The numbers are close to actual but there is a problem with null values
--
-- TODO
-- WITH aggregated_arrays as (
--   SELECT
--     submission_timestamp,
--     experiment.key AS experiment,
--     experiment.value.branch AS branch,
--     -- Materialized views do not support sub-queries for UNNESTing fields
--     -- Extract the counts from the JSON of the nested struct AS single array
--     TO_JSON_STRING(ARRAY_CONCAT(JSON_EXTRACT_ARRAY(REGEXP_REPLACE(TO_JSON_STRING(
--       REGEXP_EXTRACT_ALL(
--         TO_JSON_STRING(metrics.labeled_counter.browser_search_ad_clicks),
--         '"value":([^,}]+)'
--       )
--     ), r'"([^"])"', '[\\1, 0, 0]')),
    
--     JSON_EXTRACT_ARRAY(REGEXP_REPLACE(TO_JSON_STRING(
--       REGEXP_EXTRACT_ALL(
--         TO_JSON_STRING(metrics.labeled_counter.browser_search_with_ads),
--         '"value":([^,}]+)'
--       )
--     ), r'"([^"])"', '[0, \\1, 0]')),
    
--     JSON_EXTRACT_ARRAY(REGEXP_REPLACE(TO_JSON_STRING(
--       REGEXP_EXTRACT_ALL(
--         TO_JSON_STRING(metrics.labeled_counter.metrics_search_count),
--         '"value":([^,}]+)'
--       )
--     ), r'"([^"])"', '[0, 0, \\1]')))) AS nested_counts
--   FROM
--     `moz-fx-data-shared-prod.org_mozilla_fenix_live.metrics_v1`,
--     UNNEST(ping_info.experiments) AS experiment
--   WHERE
--     -- Limit the amount of data the materialized view is going to backfill when created.
--     -- This date can be moved forward whenever new changes of the materialized views need to be deployed.
--     DATE(submission_timestamp) > '2021-03-10'
-- )
-- select 
--   experiment, branch,
--       SUM(CAST(JSON_EXTRACT_ARRAY(counts)[OFFSET(0)] AS INT64)) AS ad_clicks_count,
--     SUM(CAST(JSON_EXTRACT_ARRAY(counts)[OFFSET(1)] AS INT64)) AS search_with_ads_count,
--     SUM(CAST(JSON_EXTRACT_ARRAY(counts)[OFFSET(2)] AS INT64)) AS search_count,
--   from aggregated_arrays, UNNEST(JSON_EXTRACT_ARRAY(REPLACE(nested_counts, '"', ""))) AS counts
--   group by experiment, branch