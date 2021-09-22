SELECT
  DATE(submission_timestamp) AS submission_date,
  # FIXME
  REGEXP_EXTRACT(_TABLE_SUFFIX, "(?:stub_)?(?:[^_]+_)(.*)__") AS dataset_id,
  COUNT(*) AS num_rows,
FROM
  `payload_bytes_decoded_all`
WHERE
  DATE(submission_timestamp) < CURRENT_DATE
  AND (@submission_date IS NULL OR @submission_date = DATE(submission_timestamp))
  AND document_type = 'deletion_request'
GROUP BY
  submission_date,
  dataset_id
