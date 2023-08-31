CREATE TEMP FUNCTION sum_map_values(map ARRAY<STRUCT<key STRING, value INT64>>)
RETURNS INT64 AS (
  (SELECT SUM(value) FROM UNNEST(map))
);

WITH client_search_activity AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    sample_id,
    client_info.client_id,
    SUM(sum_map_values(metrics.labeled_counter.search_in_content)) AS searches,
    SUM(sum_map_values(metrics.labeled_counter.browser_search_with_ads)) AS searches_with_ads,
    SUM(sum_map_values(metrics.labeled_counter.browser_search_ad_clicks)) AS ad_clicks,
  FROM
    firefox_ios.baseline
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    sample_id,
    client_id
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
  WHERE
    adjust_network NOT IN ("Unknown", "Other")
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
  COALESCE(searches, 0) AS searches,
  COALESCE(searches_with_ads, 0) AS searches_with_ads,
  COALESCE(ad_clicks, 0) AS ad_clicks,
FROM
  client_search_activity
INNER JOIN
  adjust_client_info
  USING (client_id, sample_id)
