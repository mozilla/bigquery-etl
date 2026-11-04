WITH mls AS (
  SELECT
    request_id,
    dma
  FROM
    `moz-fx-data-shared-prod.search_terms_derived.merino_log_sanitized_v4`
  WHERE
    `timestamp` >= TIMESTAMP(@submission_date)
    AND `timestamp` < TIMESTAMP(DATE_ADD(@submission_date, INTERVAL 1 DAY))
  -- Keep one row per request so the join to impressions on request_id cannot fan
  -- out; Merino logs can be redelivered with the same request_id
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY `timestamp` DESC) = 1
)
SELECT
  DATE(@submission_date) AS submission_date,
  COALESCE(mls.dma, '') AS dma,
  sis.query AS query,
  sis.block_id AS block_id,
  COUNT(sis.request_id) AS impressions,
  COUNTIF(sis.is_clicked) AS clicks,
FROM
  `moz-fx-data-shared-prod.search_terms_derived.suggest_impression_sanitized_v3` AS sis
LEFT JOIN
  mls
  USING (request_id)
WHERE
  sis.submission_timestamp >= TIMESTAMP(@submission_date)
  AND sis.submission_timestamp < TIMESTAMP(DATE_ADD(@submission_date, INTERVAL 1 DAY))
  AND LENGTH(sis.query) > 0
  AND NOT `moz-fx-data-shared-prod.udf.is_moz_supplied_suggestion`(sis.reporting_url)
GROUP BY
  sis.query,
  COALESCE(mls.dma, ''),
  sis.block_id
