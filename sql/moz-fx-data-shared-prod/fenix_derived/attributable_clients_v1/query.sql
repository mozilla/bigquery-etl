CREATE TEMP FUNCTION sum_map_values(map ARRAY<STRUCT<key STRING, value INT64>>)
RETURNS INT64 AS (
  (SELECT SUM(value) FROM UNNEST(map))
);

WITH client_day AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    sample_id,
    client_info.client_id,
    LOGICAL_OR(
      mozfun.norm.extract_version(client_info.app_display_version, 'major') >= 107
    ) AS has_search_data,
    SUM(sum_map_values(metrics.labeled_counter.browser_search_in_content)) AS searches,
    SUM(sum_map_values(metrics.labeled_counter.browser_search_with_ads)) AS searches_with_ads,
    SUM(sum_map_values(metrics.labeled_counter.browser_search_ad_clicks)) AS ad_clicks,
  FROM
    `moz-fx-data-shared-prod`.fenix.baseline
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    sample_id,
    client_id
),
searches AS (
  SELECT
    submission_date,
    client_id,
    LOGICAL_OR(mozfun.norm.extract_version(app_version, 'major') < 107) AS has_search_data,
    SUM(search_count) AS searches,
    SUM(search_with_ads) AS searches_with_ads,
    SUM(ad_click) AS ad_clicks
  FROM
    `moz-fx-data-shared-prod.search_derived.mobile_search_clients_daily_v1`
  WHERE
    submission_date = @submission_date
    AND normalized_app_name = 'Fenix'
    AND os = 'Android'
  GROUP BY
    submission_date,
    client_id
),
first_seen AS (
  SELECT
    client_id,
    country,
    first_seen_date
  FROM
    `moz-fx-data-shared-prod`.fenix.baseline_clients_first_seen
  WHERE
    submission_date >= "2021-01-01"
),
adjust_client AS (
  SELECT
    client_id,
    adjust_network,
    adjust_ad_group AS adjust_adgroup,
    adjust_campaign,
    adjust_creative,
  FROM
    `moz-fx-data-shared-prod`.fenix.firefox_android_clients
  WHERE
    adjust_network != 'Unknown'
)
SELECT
  submission_date,
  first_seen_date AS cohort_date,
  sample_id,
  client_id,
  country,
  adjust_network,
  adjust_adgroup,
  adjust_campaign,
  adjust_creative,
  first_seen_date = submission_date AS is_new_profile,
  CASE
  WHEN
    client_day.has_search_data
  THEN
    client_day.searches
  WHEN
    metrics_searches.has_search_data
  THEN
    metrics_searches.searches
  ELSE
    0
  END
  AS searches,
  CASE
  WHEN
    client_day.has_search_data
  THEN
    client_day.searches_with_ads
  WHEN
    metrics_searches.has_search_data
  THEN
    metrics_searches.searches_with_ads
  ELSE
    0
  END
  AS searches_with_ads,
  CASE
  WHEN
    client_day.has_search_data
  THEN
    client_day.ad_clicks
  WHEN
    metrics_searches.has_search_data
  THEN
    metrics_searches.ad_clicks
  ELSE
    0
  END
  AS ad_clicks,
FROM
  adjust_client
JOIN
  client_day
USING
  (client_id)
FULL OUTER JOIN
  searches AS metrics_searches
USING
  (client_id, submission_date)
JOIN
  first_seen
USING
  (client_id)
