-- Generated via ./bqetl generate experiment_monitoring
CREATE MATERIALIZED VIEW
IF
  NOT EXISTS `moz-fx-data-shared-prod.{{ dataset }}_derived.{{ destination_table }}`
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
    {% for metric_name, metric in search_metrics[dataset].items() %}
      {% if dataset == "telemetry" and metric_name == "search_count" %}
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
      {% else %}
        {% if metric == None %}
            SUM(0)
        {% else %}
            SUM(
                CAST(
                    ARRAY_CONCAT({{ metric }}, [('', 0)])[
                        SAFE_OFFSET(i)
                    ].value AS INT64
                )
            )
        {% endif %}
      AS {{ metric_name }},
      {% endif %}
    {% endfor %}
  FROM
  {% if dataset == "telemetry" %}
    `moz-fx-data-shared-prod.{{ dataset }}_live.main_v4`
    LEFT JOIN
      UNNEST(environment.experiments) AS experiment
  {% elif "_cirrus" in dataset %}
    `moz-fx-data-shared-prod.{{ dataset }}_live.enrollment_v1`
    LEFT JOIN
      UNNEST(ping_info.experiments) AS experiment
      -- We don't expect cirrus events to be search events
  {% else %}
    `moz-fx-data-shared-prod.{{ dataset }}_live.metrics_v1`
    LEFT JOIN
      UNNEST(ping_info.experiments) AS experiment
  {% endif %}
  CROSS JOIN
    -- Max. number of entries is around 10
    UNNEST(GENERATE_ARRAY(0, 50)) AS i
  WHERE
    -- Limit the amount of data the materialized view is going to backfill when created.
    -- This date can be moved forward whenever new changes of the materialized views need to be deployed.
    DATE(submission_timestamp) > '{{ start_date }}'
  GROUP BY
    submission_date,
    experiment,
    branch,
    window_start,
    window_end
