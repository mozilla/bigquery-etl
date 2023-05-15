CREATE TEMP FUNCTION sum_map_values(map ARRAY<STRUCT<key STRING, value INT64>>)
RETURNS INT64 AS (
  (SELECT SUM(value) FROM UNNEST(map))
);

WITH client_searches AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    SUM(search_count) AS searches,
    SUM(search_with_ads) AS searches_with_ads,
    SUM(ad_click) AS ad_clicks
  FROM
    search_derived.mobile_search_clients_daily_v1
  WHERE
    submission_date = @submission_date
    AND os = 'iOS'
    AND normalized_app_name = 'Fennec'
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
  WHERE
    adjust_network <> "Unknown"
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
  first_seen_date = submission_date AS is_new_install,
  first_seen_date = submission_date AS is_new_profile,
  searches,
  searches_with_ads,
  ad_clicks,
FROM
  client_searches
INNER JOIN
  adjust_client_info
USING
  (client_id, sample_id)
