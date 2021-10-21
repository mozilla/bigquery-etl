SELECT
  @submission_date AS submission_date,
  LOWER(search_query) AS query,
  block_id,
  COUNT(*) AS impressions,
  COUNTIF(is_clicked) AS clicks,
FROM
  search_terms.suggest_impression_sanitized
WHERE
  DATE(submission_timestamp)
  BETWEEN DATE_SUB(@submission_date, INTERVAL 6 DAY)
  AND @submission_date
  AND LENGTH(search_query) > 0
  AND normalized_channel = 'release'
GROUP BY
  query,
  block_id
