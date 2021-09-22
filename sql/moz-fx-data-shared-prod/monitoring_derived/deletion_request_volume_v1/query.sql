SELECT
  DATE(submission_timestamp) AS submission_date,
  REPLACE(metadata.document_namespace, '-', '_') AS dataset_id,
  COUNT(*) AS num_rows,
FROM
  monitoring.payload_bytes_decoded_all
WHERE
  DATE(submission_timestamp) < CURRENT_DATE
  AND (@submission_date IS NULL OR @submission_date = DATE(submission_timestamp))
  AND metadata.document_type = 'deletion-request'
GROUP BY
  submission_date,
  dataset_id
