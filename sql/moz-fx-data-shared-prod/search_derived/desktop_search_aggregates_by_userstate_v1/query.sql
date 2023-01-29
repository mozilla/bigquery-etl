-- Query for search_derived.desktop_search_aggregates_by_userstate_v1
SELECT
  submission_date,
  CASE
    WHEN country IN ('US', 'DE', 'FR', 'GB', 'CA')
      THEN country
    ELSE 'non-Tier1'
  END AS geo,
  CASE
    WHEN is_regular_user_v3
      THEN 'regular'
    WHEN is_new_or_resurrected_v3
      THEN 'new_or_resurrected'
    ELSE 'irregular' -- originally use 'other', but suggested to use 'irregular'
  END AS user_state,
  COUNT(client_id) AS client_count,
  COUNTIF(search_count_all > 0) AS search_client_count,
  SUM(search_count_all) AS sap,
  SUM(search_with_ads_count_all) AS search_with_ads,
  SUM(ad_clicks_count_all) AS ad_clicks,
  SUM(search_count_tagged_follow_on) AS tagged_follow_on,
  SUM(search_count_tagged_sap) AS tagged_sap,
  SUM(search_count_organic) AS organic
FROM
  telemetry.clients_last_seen
WHERE
  submission_date = @submission_date
  AND days_since_seen = 0
  AND COALESCE(search_count_all, 0) < 10000
  AND COALESCE(search_with_ads_count_all, 0) < 10000
  AND COALESCE(ad_clicks_count_all, 0) < 10000
  AND COALESCE(search_count_tagged_follow_on, 0) < 10000
  AND COALESCE(search_count_tagged_sap, 0) < 10000
  AND COALESCE(search_count_organic, 0) < 10000
GROUP BY
  1,
  2,
  3;
