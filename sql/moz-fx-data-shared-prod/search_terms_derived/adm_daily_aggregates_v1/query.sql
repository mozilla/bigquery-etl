SELECT
  @submission_date AS submission_date,
  query,
  block_id,
  COUNT(*) AS impressions,
  COUNTIF(is_clicked) AS clicks,
FROM
  `moz-fx-data-shared-prod.search_terms_derived.suggest_impression_sanitized_v3`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND LENGTH(query) > 0
  AND normalized_channel = 'release'
GROUP BY
  query,
  block_id
