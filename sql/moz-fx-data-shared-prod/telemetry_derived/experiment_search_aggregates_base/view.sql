CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_base`
AS
WITH telemetry_live_and_stable AS (
  SELECT
    *,
    'telemetry_stable' AS dataset_id
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.main_v4`
  UNION ALL
  SELECT
    *,
    'telemetry_live' AS dataset_id
  FROM
    `moz-fx-data-shared-prod.telemetry_live.main_v4`
),
desktop AS (
  SELECT
    submission_timestamp,
    dataset_id,
    unnested_experiments.key AS experiment,
    unnested_experiments.value AS branch,
    unnested_ad_clicks.value AS ad_clicks_count,
    unnested_search_with_ads.value AS search_with_ads_count,
    unnested_search_counts.count AS search_count,
  FROM
    telemetry_live_and_stable,
    UNNEST(
      ARRAY(SELECT AS STRUCT key, value.branch AS value FROM UNNEST(environment.experiments))
    ) AS unnested_experiments,
    UNNEST(payload.processes.parent.keyed_scalars.browser_search_ad_clicks) AS unnested_ad_clicks,
    UNNEST(
      payload.processes.parent.keyed_scalars.browser_search_with_ads
    ) AS unnested_search_with_ads,
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
fenix_live_and_stable AS (
  SELECT
    submission_timestamp,
    ping_info.experiments AS experiments,
    metrics.labeled_counter.browser_search_ad_clicks AS browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_with_ads AS browser_search_with_ads,
    metrics.labeled_counter.metrics_search_count AS metrics_search_count,
    'org_mozilla_fenix_stable' AS dataset_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.metrics_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    ping_info.experiments AS experiments,
    metrics.labeled_counter.browser_search_ad_clicks AS browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_with_ads AS browser_search_with_ads,
    metrics.labeled_counter.metrics_search_count AS metrics_search_count,
    'org_mozilla_fenix_live' AS dataset_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_live.metrics_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    ping_info.experiments AS experiments,
    metrics.labeled_counter.browser_search_ad_clicks AS browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_with_ads AS browser_search_with_ads,
    metrics.labeled_counter.metrics_search_count AS metrics_search_count,
    'org_mozilla_fenix_nightly_stable' AS dataset_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.metrics_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    ping_info.experiments AS experiments,
    metrics.labeled_counter.browser_search_ad_clicks AS browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_with_ads AS browser_search_with_ads,
    metrics.labeled_counter.metrics_search_count AS metrics_search_count,
    'org_mozilla_fenix_nightly_live' AS dataset_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_live.metrics_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    ping_info.experiments AS experiments,
    metrics.labeled_counter.browser_search_ad_clicks AS browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_with_ads AS browser_search_with_ads,
    metrics.labeled_counter.metrics_search_count AS metrics_search_count,
    'org_mozilla_firefox_beta_stable' AS dataset_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.metrics_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    ping_info.experiments AS experiments,
    metrics.labeled_counter.browser_search_ad_clicks AS browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_with_ads AS browser_search_with_ads,
    metrics.labeled_counter.metrics_search_count AS metrics_search_count,
    'org_mozilla_firefox_beta_live' AS dataset_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_live.metrics_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    ping_info.experiments AS experiments,
    metrics.labeled_counter.browser_search_ad_clicks AS browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_with_ads AS browser_search_with_ads,
    metrics.labeled_counter.metrics_search_count AS metrics_search_count,
    'org_mozilla_fennec_aurora_stable' AS dataset_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.metrics_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    ping_info.experiments AS experiments,
    metrics.labeled_counter.browser_search_ad_clicks AS browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_with_ads AS browser_search_with_ads,
    metrics.labeled_counter.metrics_search_count AS metrics_search_count,
    'org_mozilla_fennec_aurora_live' AS dataset_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_live.metrics_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    ping_info.experiments AS experiments,
    metrics.labeled_counter.browser_search_ad_clicks AS browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_with_ads AS browser_search_with_ads,
    metrics.labeled_counter.metrics_search_count AS metrics_search_count,
    'org_mozilla_firefox_stable' AS dataset_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.metrics_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    ping_info.experiments AS experiments,
    metrics.labeled_counter.browser_search_ad_clicks AS browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_with_ads AS browser_search_with_ads,
    metrics.labeled_counter.metrics_search_count AS metrics_search_count,
    'org_mozilla_firefox_live' AS dataset_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_live.metrics_v1`
),
fenix AS (
  SELECT
    submission_timestamp,
    dataset_id,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    unnested_ad_clicks.value AS ad_clicks_count,
    unnested_search_with_ads.value AS search_with_ads_count,
    unnested_search_counts.value AS search_count,
  FROM
    fenix_live_and_stable,
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
  date(submission_timestamp) AS submission_date,
  dataset_id,
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
GROUP BY
  submission_date,
  dataset_id,
  experiment,
  branch,
  window_start,
  window_end
