SELECT
  search_query AS query,
  block_id,
  COUNT(*) AS impressions,
  COUNTIF(is_clicked) AS clicks,
FROM
  search_terms.suggest_impression_sanitized
WHERE
  DATE(submission_timestamp)
  BETWEEN @submission_date
  AND DATE_SUB(@submission_date, INTERVAL 6 DAY)
GROUP BY
  query,
  block_id,
  position
HAVING
  -- This filter matches aggregated_search_terms_daily, but may change; see
  -- https://bugzilla.mozilla.org/show_bug.cgi?id=1729524
  COUNT(DISTINCT context_id) > 30000
