CREATE MATERIALIZED VIEW
IF
  NOT EXISTS `moz-fx-data-shared-prod.telemetry_derived.experiment_search_events_live_v1`
  OPTIONS
    (enable_refresh = TRUE, refresh_interval_minutes = 5)
  AS
  WITH event_counts AS (
    SELECT
        submission_timestamp,
        experiment.key AS experiment,
        experiment.value.branch AS branch,
        -- Materialized views do not support sub-queries for UNNESTing fields
        -- Extract the counts from the JSON of the nested struct AS single array
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
  ) SELECT 
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

