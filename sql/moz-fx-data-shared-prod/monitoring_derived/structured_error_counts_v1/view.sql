CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring_derived.structured_error_counts_v1`
AS
WITH ping_counts AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    metadata.document_namespace,
    metadata.document_type,
    metadata.document_version,
    COUNT(*) AS ping_count
  FROM
    `moz-fx-data-shared-prod.monitoring.payload_bytes_decoded_structured`
  WHERE
    submission_timestamp >= TIMESTAMP_SUB(current_timestamp, INTERVAL(28 * 24) HOUR)
  GROUP BY
    hour,
    document_namespace,
    document_type,
    document_version
),
error_counts AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    document_namespace,
    document_type,
    document_version,
    error_type,
    COUNT(*) AS error_count
  FROM
    `moz-fx-data-shared-prod.monitoring.payload_bytes_error_structured`
  WHERE
    submission_timestamp >= TIMESTAMP_SUB(current_timestamp, INTERVAL(28 * 24) HOUR)
  GROUP BY
    hour,
    document_namespace,
    document_type,
    document_version,
    error_type
),
structured_hourly_errors AS (
  SELECT
    hour,
    document_namespace,
    document_type,
    document_version,
    error_type,
    COALESCE(ping_count, 0) + COALESCE(error_count, 0) AS ping_count,
    COALESCE(error_count, 0) AS error_count
  FROM
    ping_counts
  FULL OUTER JOIN
    error_counts
  USING
    (hour, document_namespace, document_type, document_version)
),
with_ratio AS (
  SELECT
    *,
    SAFE_DIVIDE(1.0 * error_count, ping_count) AS error_ratio
  FROM
    structured_hourly_errors
)
SELECT
  *
FROM
  with_ratio
