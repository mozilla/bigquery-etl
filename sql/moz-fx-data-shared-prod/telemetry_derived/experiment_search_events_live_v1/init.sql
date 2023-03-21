-- Generated via ./bqetl generate experiment_monitoring
CREATE MATERIALIZED VIEW
IF
  NOT EXISTS `moz-fx-data-shared-prod.telemetry_derived.experiment_search_events_live_v1`
  OPTIONS
    (enable_refresh = TRUE, refresh_interval_minutes = 5)
  AS
  SELECT
    DATE(submission_timestamp) AS submission_date,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
      -- Aggregates event counts over 5-minute intervals
      INTERVAL(DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) * 5) MINUTE
    ) AS window_start,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
      INTERVAL((DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) + 1) * 5) MINUTE
    ) AS window_end,
    -- Concatenating an element with value = 0 ensures that the count values are not null even if the array is empty
    -- Materialized views don't support COALESCE or IFNULL
    SUM(
      CAST(
        ARRAY_CONCAT(payload.processes.parent.keyed_scalars.browser_search_ad_clicks, [('', 0)])[
          SAFE_OFFSET(i)
        ].value AS INT64
      )
    ) AS ad_clicks_count,
    SUM(
      CAST(
        ARRAY_CONCAT(payload.processes.parent.keyed_scalars.browser_search_with_ads, [('', 0)])[
          SAFE_OFFSET(i)
        ].value AS INT64
      )
    ) AS search_with_ads_count,
    SUM(
      IF(
        REGEXP_CONTAINS(
          payload.keyed_histograms.search_counts[SAFE_OFFSET(i)].value,
          r'"sum":([^},]+)'
        ),
        CAST(
          REGEXP_EXTRACT(
            payload.keyed_histograms.search_counts[SAFE_OFFSET(i)].value,
            r'"sum":([^},]+)'
          ) AS INT64
        ),
        0
      ) + IF(
        REGEXP_CONTAINS(payload.keyed_histograms.search_counts[SAFE_OFFSET(i)].value, r'^"(\d+)"$'),
        CAST(
          REGEXP_EXTRACT(
            payload.keyed_histograms.search_counts[SAFE_OFFSET(i)].value,
            r'^"(\d+)"$'
          ) AS INT64
        ),
        0
      ) + IF(
        REGEXP_CONTAINS(
          payload.keyed_histograms.search_counts[SAFE_OFFSET(i)].value,
          r'^"\d+;(\d+);'
        ),
        CAST(
          REGEXP_EXTRACT(
            payload.keyed_histograms.search_counts[SAFE_OFFSET(i)].value,
            r'^"\d+;(\d+);'
          ) AS INT64
        ),
        0
      ) + IF(
        REGEXP_CONTAINS(
          payload.keyed_histograms.search_counts[SAFE_OFFSET(i)].value,
          r'^"(\d+),\d+"'
        ),
        CAST(
          REGEXP_EXTRACT(
            payload.keyed_histograms.search_counts[SAFE_OFFSET(i)].value,
            r'^"(\d+),\d+"'
          ) AS INT64
        ),
        0
      )
    ) AS search_count,
  FROM
    `moz-fx-data-shared-prod.telemetry_live.main_v4`
  LEFT JOIN
    UNNEST(environment.experiments) AS experiment
  CROSS JOIN
    -- Max. number of entries is around 10
    UNNEST(GENERATE_ARRAY(0, 50)) AS i
  WHERE
    -- Limit the amount of data the materialized view is going to backfill when created.
    -- This date can be moved forward whenever new changes of the materialized views need to be deployed.
    DATE(submission_timestamp) > '2023-03-13'
  GROUP BY
    submission_date,
    experiment,
    branch,
    window_start,
    window_end
