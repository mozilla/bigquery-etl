-- Return a stratified sample of documents within the payload_bytes_decoded and
-- payload_bytes_error tables. This query can be used for testing the expected
-- behavior of the ingestion pipeline and for validating changes when updating
-- schemas.
WITH extract_decoded AS (
  SELECT
    metadata.document_namespace,
    metadata.document_type,
    metadata.document_version,
    '' AS error_message,
    payload
  FROM
    `moz-fx-data-shar-nonprod-efed.payload_bytes_decoded.*`
  WHERE
    submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY)
),
extract_error AS (
  SELECT
    document_namespace,
    document_type,
    document_version,
    error_message,
    payload
  FROM
    `moz-fx-data-shar-nonprod-efed.payload_bytes_error.*`
  WHERE
    submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY)
    AND error_type = 'ParsePayload'
),
extracted AS (
  SELECT
    *
  FROM
    extract_decoded
  UNION ALL
  SELECT
    *
  FROM
    extract_error
)
SELECT
  CURRENT_TIMESTAMP() AS submission_timestamp,
  ARRAY_LENGTH(samples) AS n_samples,
  document_decoded,
  sample.*
FROM
  (
    SELECT
      document_namespace,
      document_type,
      document_version,
      BYTE_LENGTH(error_message) = 0 AS document_decoded,
      ARRAY_AGG(extracted ORDER BY RAND() LIMIT 1000) samples
    FROM
      extracted
    GROUP BY
      document_namespace,
      document_type,
      document_version,
      document_decoded
    HAVING
      ARRAY_LENGTH(samples) > 10
  ),
  UNNEST(samples) sample
