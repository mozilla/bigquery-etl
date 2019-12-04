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
    submission_timestamp
> TIMESTAMP_SUB(current_timestamp, INTERVAL 1 day)
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
    submission_timestamp
> TIMESTAMP_SUB(current_timestamp, INTERVAL 1 day)
AND error_type = 'ParsePayload'
)
SELECT
  n_samples,
  document_is_decoded,
  sample.*
FROM
  (
    SELECT
      document_namespace,
      document_type,
      document_version,
      BYTE_LENGTH(error_message) = 0 AS document_is_decoded,
      ARRAY_AGG(
        e
        ORDER BY
          rand()
        LIMIT
          1000
      ) samples, COUNT(*) AS n_samples
    FROM
      (
        SELECT
          *
        FROM
          extract_decoded
        UNION ALL
        SELECT
          *
        FROM
          extract_error
      ) e
    GROUP BY
      document_namespace,
      document_type,
      document_version,
      document_is_decoded
    HAVING
      n_samples
  > 10
ORDER BY
  n_samples
),
UNNEST(samples) sample
