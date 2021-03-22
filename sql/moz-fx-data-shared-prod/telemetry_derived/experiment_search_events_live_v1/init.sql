CREATE MATERIALIZED VIEW
IF
  NOT EXISTS `moz-fx-data-shared-prod.telemetry_derived.experiment_search_events_live_v1`
  OPTIONS
    (enable_refresh = TRUE, refresh_interval_minutes = 5)
  AS
    -- Materialized views do not support sub-queries for UNNESTing fields.
  -- This limitation prevents summing up individual nested counters.
  -- As a workaround the nested counters are converted into arrays of counts. The
  -- conversion is done by a combination of dumping the fields as JSON
  -- strings, applying regexes and concatenating extracted strings.
  --
  -- (1) Dump the nested counter field as JSON string, for example:
  --     [{"key":"duckduckgo","value":7}, {"key":"google","value":4}]
  -- (2) Using a regex, extract all values and dump the result array as
  --     JSON string: ["7", "4"]
  -- (3) Using regex replace, transform the count numbers in the array
  --     to an array where the position of the element determines the
  --     count type [search_ad_clicks, search_with_ads, search_count]:
  --     "[[7, 0, 0], [4, 0, 0]]"
  -- (4) Convert the nested array string to an array and concatenate
  --     nested arrays of ping to a single array and dump as JSON string,
  --     for example: "[[7, 0, 0], [4, 0, 0], [0, 1, 0], [0, 0, 3]]"
  --
  -- (5) Convert the nested array string into an array, unnest and sum
  --     individual counts based on their array index:
  --        ad_clicks_count = 11
  --        search_with_ads_count = 1
  --        search_count = 3
  WITH aggregated_arrays AS (
    SELECT
      submission_timestamp,
      experiment.key AS experiment,
      experiment.value.branch AS branch,
      -- (4)
      TO_JSON_STRING(
        ARRAY_CONCAT(
          JSON_EXTRACT_ARRAY(
            -- (3)
            REGEXP_REPLACE(
              -- (2)
              TO_JSON_STRING(
                REGEXP_EXTRACT_ALL(
                  -- (1)
                  TO_JSON_STRING(payload.processes.parent.keyed_scalars.browser_search_ad_clicks),
                  '"value":([^,}]+)'
                )
              ),
              r'"([^"]+)"',
              r'[\1, 0, 0]'
            )
          ),
          JSON_EXTRACT_ARRAY(
            -- (3)
            REGEXP_REPLACE(
              -- (2)
              TO_JSON_STRING(
                REGEXP_EXTRACT_ALL(
                  -- (1)
                  TO_JSON_STRING(payload.processes.parent.keyed_scalars.browser_search_with_ads),
                  '"value":([^,}]+)'
                )
              ),
              r'"([^"]+)"',
              r'[0, \1, 0]'
            )
          ),
          JSON_EXTRACT_ARRAY(
            -- (3)
            REGEXP_REPLACE(
              -- (2)
              CONCAT(
                TO_JSON_STRING(
                  ARRAY_CONCAT(
                    -- extract sum from histogram
                    -- acount for compact and JSON histograms
                    REGEXP_EXTRACT_ALL(
                      -- (1)
                      TO_JSON_STRING(payload.keyed_histograms.search_counts),
                      r'\\"sum\\":([^},]+)'
                    ),
                    REGEXP_EXTRACT_ALL(
                      -- (1)
                      TO_JSON_STRING(payload.keyed_histograms.search_counts),
                      r'"value":"(\d+)"'
                    ),
                    REGEXP_EXTRACT_ALL(
                      -- (1)
                      TO_JSON_STRING(payload.keyed_histograms.search_counts),
                      r'"value":"\d+;(\d+);'
                    ),
                    REGEXP_EXTRACT_ALL(
                      -- (1)
                      TO_JSON_STRING(payload.keyed_histograms.search_counts),
                      r'"value":"(\d+),\d+"'
                    )
                  )
                )
              ),
              r'"([^"]+)"',
              r'[0, 0, \1]'
            )
          )
        )
      ) AS nested_counts
    FROM
      `moz-fx-data-shared-prod.telemetry_live.main_v4`
    LEFT JOIN
      UNNEST(environment.experiments) AS experiment
    WHERE
    -- Limit the amount of data the materialized view is going to backfill when created.
    -- This date can be moved forward whenever new changes of the materialized views need to be deployed.
      DATE(submission_timestamp) > '2021-03-12'
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
    -- (5)
    SUM(CAST(JSON_EXTRACT_ARRAY(counts)[OFFSET(0)] AS INT64)) AS ad_clicks_count,
    SUM(CAST(JSON_EXTRACT_ARRAY(counts)[OFFSET(1)] AS INT64)) AS search_with_ads_count,
    SUM(CAST(JSON_EXTRACT_ARRAY(counts)[OFFSET(2)] AS INT64)) AS search_count,
  FROM
    aggregated_arrays,
    UNNEST(JSON_EXTRACT_ARRAY(REPLACE(nested_counts, '"', ""))) AS counts
  GROUP BY
    submission_date,
    experiment,
    branch,
    window_start,
    window_end
