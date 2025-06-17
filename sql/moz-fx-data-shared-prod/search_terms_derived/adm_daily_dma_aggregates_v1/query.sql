SELECT
  @submission_date AS submission_date,
  COALESCE(mls.dma, '') AS dma,
  sis.query AS query,
  sis.block_id AS block_id,
  COUNT(sis.request_id) AS impressions,
  COUNTIF(sis.is_clicked) AS clicks,
FROM
  `moz-fx-data-shared-prod.search_terms_derived.suggest_impression_sanitized_v3` AS sis
LEFT JOIN
  `moz-fx-data-shared-prod.search_terms_derived.merino_log_sanitized_v3` AS mls
  ON (sis.request_id = mls.request_id)
WHERE
  DATE(sis.submission_timestamp) = @submission_date
  AND LENGTH(sis.query) > 0
  AND sis.normalized_channel = 'release'
  AND DATE(sis.submission_timestamp) = @submission_date
GROUP BY
  sis.query,
  COALESCE(mls.dma, ''),
  sis.block_id
