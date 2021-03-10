WITH desktop AS (
  SELECT
    submission_timestamp,
    unnested_experiments.key AS experiment,
    unnested_experiments.value AS branch,
    unnested_ad_clicks.value AS ad_clicks_count,
    unnested_search_with_ads.value AS search_with_ads_count,
    unnested_search_counts.count AS search_count,
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.main_v4`
  LEFT JOIN
    UNNEST(
      ARRAY(SELECT AS STRUCT key, value.branch AS value FROM UNNEST(environment.experiments))
    ) AS unnested_experiments
  LEFT JOIN
    UNNEST(payload.processes.parent.keyed_scalars.browser_search_ad_clicks) AS unnested_ad_clicks
  LEFT JOIN
    UNNEST(
      payload.processes.parent.keyed_scalars.browser_search_with_ads
    ) AS unnested_search_with_ads
  LEFT JOIN
    UNNEST(
      ARRAY(
        SELECT AS STRUCT
          SUBSTR(_key, 0, pos - 2) AS engine,
          SUBSTR(_key, pos) AS source,
          `moz-fx-data-shared-prod`.udf.extract_histogram_sum(value) AS `count`
        FROM
          UNNEST(payload.keyed_histograms.search_counts),
          UNNEST([REPLACE(key, 'in-content.', 'in-content:')]) AS _key,
          UNNEST([LENGTH(REGEXP_EXTRACT(_key, '.+[.].'))]) AS pos
      )
    ) AS unnested_search_counts
),
fenix_search AS (
  SELECT
    submission_timestamp,
    ping_info.experiments AS experiments,
    metrics.labeled_counter.browser_search_ad_clicks AS browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_with_ads AS browser_search_with_ads,
    metrics.labeled_counter.metrics_search_count AS metrics_search_count
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.metrics_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    ping_info.experiments AS experiments,
    metrics.labeled_counter.browser_search_ad_clicks AS browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_with_ads AS browser_search_with_ads,
    metrics.labeled_counter.metrics_search_count AS metrics_search_count
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.metrics_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    ping_info.experiments AS experiments,
    metrics.labeled_counter.browser_search_ad_clicks AS browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_with_ads AS browser_search_with_ads,
    metrics.labeled_counter.metrics_search_count AS metrics_search_count
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.metrics_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    ping_info.experiments AS experiments,
    metrics.labeled_counter.browser_search_ad_clicks AS browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_with_ads AS browser_search_with_ads,
    metrics.labeled_counter.metrics_search_count AS metrics_search_count
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.metrics_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    ping_info.experiments AS experiments,
    metrics.labeled_counter.browser_search_ad_clicks AS browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_with_ads AS browser_search_with_ads,
    metrics.labeled_counter.metrics_search_count AS metrics_search_count
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.metrics_v1`
),
fenix AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    unnested_ad_clicks.value AS ad_clicks_count,
    unnested_search_with_ads.value AS search_with_ads_count,
    unnested_search_counts.value AS search_count,
  FROM
    fenix_search,
    UNNEST(browser_search_ad_clicks) AS unnested_ad_clicks,
    UNNEST(browser_search_with_ads) AS unnested_search_with_ads,
    UNNEST(metrics_search_count) AS unnested_search_counts,
    UNNEST(experiments) AS experiment
),
all_events AS (
  SELECT
    *
  FROM
    desktop
  UNION ALL
  SELECT
    *
  FROM
    fenix
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
  date(submission_timestamp) = @submission_date
GROUP BY
  experiment,
  branch,
  window_start,
  window_end
