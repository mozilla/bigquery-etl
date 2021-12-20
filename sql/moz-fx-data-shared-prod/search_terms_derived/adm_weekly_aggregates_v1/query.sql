SELECT
  DATE_SUB(@submission_date, INTERVAL 6 DAY) AS start_date,
  @submission_date AS submission_date,
  sanitized_query AS query,
  block_id,
  COUNT(*) AS impressions,
  COUNTIF(is_clicked) AS clicks,
FROM
  search_terms_derived.suggest_impression_sanitized_v2
WHERE
  DATE(submission_timestamp)
  BETWEEN DATE_SUB(@submission_date, INTERVAL 6 DAY)
  AND @submission_date
  AND LENGTH(sanitized_query) > 0
  AND normalized_channel = 'release'
GROUP BY
  query,
  block_id
