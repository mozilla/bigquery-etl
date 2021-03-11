-- This and the following materialized view need to be kept in sync:
-- - org_mozilla_fenix_derived.experiment_search_events_live_v1
-- - org_mozilla_firefox_beta_derived.experiment_search_events_live_v1
-- - org_mozilla_firefox_derived.experiment_search_events_live_v1
CREATE MATERIALIZED VIEW
IF
  NOT EXISTS `moz-fx-data-shared-prod.org_mozilla_fenix_derived.experiment_search_events_live_v1`
  OPTIONS
    (enable_refresh = TRUE, refresh_interval_minutes = 5)
  AS
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
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
    `moz-fx-data-shared-prod.org_mozilla_fenix_live.metrics_v1`,
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
