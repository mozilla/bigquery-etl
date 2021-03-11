CREATE MATERIALIZED VIEW
IF
  NOT EXISTS `moz-fx-data-shared-prod.telemetry_derived.experiment_search_events_live_v1`
  OPTIONS
    (enable_refresh = TRUE, refresh_interval_minutes = 5)
  AS
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    TO_JSON_STRING(
      REGEXP_EXTRACT_ALL(
        TO_JSON_STRING(payload.processes.parent.keyed_scalars.browser_search_ad_clicks),
        '"value":([^,}]+)'
      )
    ) AS ad_clicks,
    TO_JSON_STRING(
      REGEXP_EXTRACT_ALL(
        TO_JSON_STRING(payload.processes.parent.keyed_scalars.browser_search_with_ads),
        '"value":([^,}]+)'
      )
    ) AS search_with_ads,
    TO_JSON_STRING(
      REGEXP_EXTRACT_ALL(TO_JSON_STRING(payload.keyed_histograms.search_counts), '"value":([^,}]+)')
    ) AS search_count
  FROM
    `moz-fx-data-shared-prod.telemetry_live.main_v4`,
    UNNEST(environment.experiments) AS experiment
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
