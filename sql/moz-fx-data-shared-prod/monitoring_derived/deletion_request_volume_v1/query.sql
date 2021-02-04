SELECT
  DATE(submission_timestamp) AS submission_date,
  REGEXP_EXTRACT(_TABLE_SUFFIX, "(?:stub_)?(?:[^_]+_)(.*)__") AS dataset_id,
  COUNT(*) AS num_rows,
FROM
  `payload_bytes_decoded.*`
WHERE
  DATE(submission_timestamp) < CURRENT_DATE
  AND (@submission_date IS NULL OR @submission_date = DATE(submission_timestamp))
  AND _TABLE_SUFFIX LIKE '%__deletion_request_v%'
GROUP BY
  submission_date,
  dataset_id
