WITH ping_counts AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    metadata.document_namespace,
    metadata.document_type,
    metadata.document_version,
    COUNT(*) AS ping_count
  FROM
    `payload_bytes_decoded.structured_*`
  WHERE
    submission_timestamp >= TIMESTAMP_SUB(current_timestamp, INTERVAL 28 * 24 HOUR)
  GROUP BY hour, document_namespace, document_type, document_version
), error_counts AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    document_namespace,
    document_type,
    document_version,
    error_type,
    COUNT(*) AS error_count
  FROM
    payload_bytes_error.structured
  WHERE
    submission_timestamp >= TIMESTAMP_SUB(current_timestamp, INTERVAL 28 * 24 HOUR)
  GROUP BY hour, document_namespace, document_type, document_version, error_type
), structured_hourly_errors AS (
  SELECT
    hour,
    document_namespace,
    document_type,
    document_version,
    error_type,
    ping_count + error_counts.error_count AS ping_count,
    error_counts.error_count,
    SAFE_DIVIDE(1.0 * error_counts.error_count, error_counts.error_count + ping_count) AS error_ratio
  FROM
    ping_counts
  INNER JOIN
    error_counts USING (hour, document_namespace, document_type, document_version)
)

CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.pipeline.structured_error_counts_v1`
AS SELECT * FROM
  structured_hourly_errors
