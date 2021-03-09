-- This and the following materialized view need to be kept in sync:
-- - org_mozilla_fenix_derived.experiment_search_events_live_v1
-- - org_mozilla_fenix_beta_derived.experiment_search_events_live_v1
-- - org_mozilla_firefox_derived.experiment_search_events_live_v1
CREATE MATERIALIZED VIEW
IF
  NOT EXISTS `moz-fx-data-shared-prod.org_mozilla_firefox_derived.experiment_search_events_live_v1`
  OPTIONS
    (enable_refresh = TRUE, refresh_interval_minutes = 5)
  AS
  WITH fenix AS (
    SELECT
      submission_timestamp,
      experiment.key AS experiment,
      experiment.value.branch AS branch,
      unnested_ad_clicks.value AS ad_clicks_count,
      unnested_search_with_ads.value AS search_with_ads_count,
      unnested_search_counts.value AS search_count,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_firefox_live.metrics_v1`,
      UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS unnested_ad_clicks,
      UNNEST(metrics.labeled_counter.browser_search_with_ads) AS unnested_search_with_ads,
      UNNEST(metrics.labeled_counter.metrics_search_count) AS unnested_search_counts,
      UNNEST(ping_info.experiments) AS experiment
  )
  SELECT
    date(submission_timestamp) AS submission_date,
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
    SUM(ad_clicks_count) AS ad_clicks_count,
    SUM(search_with_ads_count) AS search_with_ads_count,
    SUM(search_count) AS search_count,
  FROM
    fenix
  WHERE
    -- Limit the amount of data the materialized view is going to backfill when created.
    -- This date can be moved forward whenever new changes of the materialized views need to be deployed.
    DATE(submission_timestamp) >= DATE('2021-03-01')
  GROUP BY
    submission_date,
    experiment,
    branch,
    window_start,
    window_end
