-- Generated via ./bqetl generate experiment_monitoring
WITH
{% for app_dataset in applications %}
  {% if app_dataset == "telemetry" %}
  {{ app_dataset }} AS (
    SELECT
      submission_timestamp,
      unnested_experiments.key AS experiment,
      unnested_experiments.value AS branch,
      SUM(
        (
          SELECT
            SUM(value.value)
          FROM
            UNNEST(payload.processes.parent.keyed_scalars.browser_search_ad_clicks) AS value
        )
      ) AS ad_clicks_count,
      SUM(
        (
          SELECT
            SUM(value.value)
          FROM
            UNNEST(payload.processes.parent.keyed_scalars.browser_search_with_ads) AS value
        )
      ) AS search_with_ads_count,
      SUM(
        (
          SELECT
            SUM(`moz-fx-data-shared-prod`.udf.extract_histogram_sum(value.value))
          FROM
            UNNEST(payload.keyed_histograms.search_counts) AS value
        )
      ) AS search_count
    FROM
      `moz-fx-data-shared-prod.telemetry_stable.main_v5`
    LEFT JOIN
      UNNEST(
        ARRAY(SELECT AS STRUCT key, value.branch AS value FROM UNNEST(environment.experiments))
      ) AS unnested_experiments
    GROUP BY
      submission_timestamp,
      experiment,
      branch
  ),
  {% else %}
  {{ app_dataset }} AS (
    SELECT
      submission_timestamp,
      experiment.key AS experiment,
      experiment.value.branch AS branch,
      {% for metric_name, metric in search_metrics[app_dataset].items() %}
        {% if metric == None %}
          SUM(0)
        {% else %}
          SUM(
            (
              SELECT
                SUM(value.value)
              FROM
                UNNEST({{ metric }}) AS value
            )
          )
        {% endif %}
        AS {{ metric_name }},
      {% endfor %}
    FROM
      `moz-fx-data-shared-prod.{{ app_dataset }}_stable.metrics_v1`
    LEFT JOIN
      UNNEST(ping_info.experiments) AS experiment
    GROUP BY
      submission_timestamp,
      experiment,
      branch
  ),
  {% endif %}
{% endfor %}
all_events AS (
  {% for app_dataset in applications %}
    SELECT
      *
    FROM
      {{ app_dataset }}
    {% if not loop.last %}
      UNION ALL
    {% endif %}
  {% endfor %}
)
SELECT
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
  all_events
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  experiment,
  branch,
  window_start,
  window_end
