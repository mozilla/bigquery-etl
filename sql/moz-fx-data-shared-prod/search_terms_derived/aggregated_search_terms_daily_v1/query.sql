SELECT
  DATE(submission_timestamp) AS submission_date,
  LOWER(search_query) AS search_terms,
  COUNT(*) AS impressions,
  COUNTIF(is_clicked) AS clicks,
  COUNT(DISTINCT context_id) AS client_days
FROM
  `moz-fx-data-shared-prod.contextual_services_stable.quicksuggest_impression_v1`
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  search_terms
HAVING
  client_days > 30000
