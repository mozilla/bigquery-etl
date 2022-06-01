SELECT
  DATE(submission_timestamp) AS submission_date,
  sanitized_query AS search_terms,
  COUNT(*) AS impressions,
  COUNTIF(is_clicked) AS clicks,
  COUNT(DISTINCT context_id) AS client_days
FROM
  search_terms_derived.suggest_impression_sanitized_v2
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  search_terms
