CREATE TEMP FUNCTION sum_map_values(map ARRAY<STRUCT<key STRING, value INT64>>)
RETURNS INT64 AS (
  (SELECT SUM(value) FROM UNNEST(map))
);

WITH client_days AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    sample_id,
    client_info.client_id,
    LOGICAL_OR(mozfun.norm.extract_version(app_version, 'major') >= 108) AS has_search_data,
    SUM(sum_map_values(metrics.labeled_counter.search_in_content)) AS searches,
    SUM(sum_map_values(metrics.labeled_counter.browser_search_with_ads)) AS searches_with_ads,
    SUM(sum_map_values(metrics.labeled_counter.browser_search_ad_clicks)) AS ad_clicks,
  FROM
    firefox_ios.baseline
  WHERE
    {% if is_init() %}
      submission_date >= "2020-05-01"
    {% else %}
      submission_date = @submission_date
    {% endif %}
  GROUP BY
    submission_date,
    sample_id,
    client_id
),
metrics_searches AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    LOGICAL_OR(mozfun.norm.extract_version(app_version, 'major') < 108) AS has_search_data,
    SUM(search_count) AS searches,
    SUM(search_with_ads) AS searches_with_ads,
    SUM(ad_click) AS ad_clicks
  FROM
    search_derived.mobile_search_clients_daily_v1
  WHERE
    {% if is_init() %}
      submission_date >= "2020-05-01"
    {% else %}
      submission_date = @submission_date
    {% endif %}
    AND normalized_app_name = 'Fennec'
    AND os = 'iOS'
  GROUP BY
    submission_date,
    client_id,
    sample_id
),
adjust_client_info AS (
  SELECT
    client_id,
    sample_id,
    first_seen_date,
    adjust_network,
    adjust_ad_group AS adjust_adgroup,
    adjust_campaign,
    adjust_creative,
    metadata.is_reported_first_session_ping,
  FROM
    firefox_ios.firefox_ios_clients
)
SELECT
  submission_date,
  first_seen_date,
  sample_id,
  client_id,
  adjust_network,
  adjust_adgroup,
  adjust_campaign,
  adjust_creative,
  first_seen_date = submission_date AS is_new_profile,
  CASE
    WHEN client_days.has_search_data
      THEN client_days.searches
    WHEN metrics_searches.has_search_data
      THEN metrics_searches.searches
    ELSE 0
  END AS searches,
  CASE
    WHEN client_days.has_search_data
      THEN client_days.searches_with_ads
    WHEN metrics_searches.has_search_data
      THEN metrics_searches.searches_with_ads
    ELSE 0
  END AS searches_with_ads,
  CASE
    WHEN client_days.has_search_data
      THEN client_days.ad_clicks
    WHEN metrics_searches.has_search_data
      THEN metrics_searches.ad_clicks
    ELSE 0
  END AS ad_clicks,
FROM
  client_days
FULL OUTER JOIN
  metrics_searches
USING
  (client_id, sample_id, submission_date)
INNER JOIN
  adjust_client_info
USING
  (client_id, sample_id)
