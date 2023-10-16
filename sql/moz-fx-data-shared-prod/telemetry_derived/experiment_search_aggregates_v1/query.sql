-- Generated via ./bqetl generate experiment_monitoring
WITH org_mozilla_firefox_beta AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    SUM(
      (
        SELECT
          SUM(value.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS value
      )
    ) AS ad_clicks_count,
    SUM(
      (
        SELECT
          SUM(value.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_with_ads) AS value
      )
    ) AS search_with_ads_count,
    SUM(
      (SELECT SUM(value.value) FROM UNNEST(metrics.labeled_counter.metrics_search_count) AS value)
    ) AS search_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.metrics_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch
),
org_mozilla_fenix AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    SUM(
      (
        SELECT
          SUM(value.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS value
      )
    ) AS ad_clicks_count,
    SUM(
      (
        SELECT
          SUM(value.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_with_ads) AS value
      )
    ) AS search_with_ads_count,
    SUM(
      (SELECT SUM(value.value) FROM UNNEST(metrics.labeled_counter.metrics_search_count) AS value)
    ) AS search_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.metrics_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch
),
org_mozilla_firefox AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    SUM(
      (
        SELECT
          SUM(value.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS value
      )
    ) AS ad_clicks_count,
    SUM(
      (
        SELECT
          SUM(value.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_with_ads) AS value
      )
    ) AS search_with_ads_count,
    SUM(
      (SELECT SUM(value.value) FROM UNNEST(metrics.labeled_counter.metrics_search_count) AS value)
    ) AS search_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.metrics_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch
),
org_mozilla_ios_firefox AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    SUM(0) AS ad_clicks_count,
    SUM(0) AS search_with_ads_count,
    SUM(
      (SELECT SUM(value.value) FROM UNNEST(metrics.labeled_counter.search_counts) AS value)
    ) AS search_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.metrics_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch
),
org_mozilla_ios_firefoxbeta AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    SUM(0) AS ad_clicks_count,
    SUM(0) AS search_with_ads_count,
    SUM(
      (SELECT SUM(value.value) FROM UNNEST(metrics.labeled_counter.search_counts) AS value)
    ) AS search_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.metrics_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch
),
org_mozilla_ios_fennec AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    SUM(0) AS ad_clicks_count,
    SUM(0) AS search_with_ads_count,
    SUM(
      (SELECT SUM(value.value) FROM UNNEST(metrics.labeled_counter.search_counts) AS value)
    ) AS search_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.metrics_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch
),
telemetry AS (
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
org_mozilla_klar AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    SUM(
      (
        SELECT
          SUM(value.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS value
      )
    ) AS ad_clicks_count,
    SUM(
      (
        SELECT
          SUM(value.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_with_ads) AS value
      )
    ) AS search_with_ads_count,
    SUM(0) AS search_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_klar_stable.metrics_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch
),
org_mozilla_focus AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    SUM(
      (
        SELECT
          SUM(value.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS value
      )
    ) AS ad_clicks_count,
    SUM(
      (
        SELECT
          SUM(value.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_with_ads) AS value
      )
    ) AS search_with_ads_count,
    SUM(0) AS search_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_stable.metrics_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch
),
org_mozilla_focus_nightly AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    SUM(
      (
        SELECT
          SUM(value.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS value
      )
    ) AS ad_clicks_count,
    SUM(
      (
        SELECT
          SUM(value.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_with_ads) AS value
      )
    ) AS search_with_ads_count,
    SUM(0) AS search_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_nightly_stable.metrics_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch
),
org_mozilla_focus_beta AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    SUM(
      (
        SELECT
          SUM(value.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS value
      )
    ) AS ad_clicks_count,
    SUM(
      (
        SELECT
          SUM(value.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_with_ads) AS value
      )
    ) AS search_with_ads_count,
    SUM(0) AS search_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_beta_stable.metrics_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch
),
org_mozilla_ios_klar AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    SUM(0) AS ad_clicks_count,
    SUM(0) AS search_with_ads_count,
    SUM(0) AS search_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_klar_stable.metrics_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch
),
org_mozilla_ios_focus AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    SUM(0) AS ad_clicks_count,
    SUM(0) AS search_with_ads_count,
    SUM(0) AS search_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_focus_stable.metrics_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch
),
monitor_cirrus AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    SUM(0) AS ad_clicks_count,
    SUM(0) AS search_with_ads_count,
    SUM(0) AS search_count,
  FROM
    `moz-fx-data-shared-prod.monitor_cirrus_stable.metrics_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch
),
all_events AS (
  SELECT
    *
  FROM
    org_mozilla_firefox_beta
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_fenix
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_firefox
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_ios_firefox
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_ios_firefoxbeta
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_ios_fennec
  UNION ALL
  SELECT
    *
  FROM
    telemetry
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_klar
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_focus
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_focus_nightly
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_focus_beta
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_ios_klar
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_ios_focus
  UNION ALL
  SELECT
    *
  FROM
    monitor_cirrus
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
