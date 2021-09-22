SELECT
  DATE(submission_timestamp) AS submission_date,
  search_query AS search_terms,
  COUNT(*) AS impressions,
  COUNTIF(is_clicked) AS clicks,
  COUNT(DISTINCT context_id) AS client_days
FROM
  `moz-fx-data-shared-prod.contextual_services_stable.quicksuggest_impression_v1`
GROUP BY
  submission_date,
  search_query
HAVING
  client_days > 30000
