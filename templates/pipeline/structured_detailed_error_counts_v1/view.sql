CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.pipeline.detailed_structured_error_counts_v1`
AS
WITH error_examples AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    document_namespace,
    document_type,
    document_version,
    error_type,
    error_message,
    udf_js_gunzip(ARRAY_AGG(payload)[OFFSET(0)]) AS sample_payload,
    COUNT(*) AS error_count
  FROM
    payload_bytes_error.structured
  WHERE
    submission_timestamp >= TIMESTAMP_SUB(current_timestamp, INTERVAL 28 * 24 HOUR)
  GROUP BY hour, document_namespace, document_type, document_version, error_type, error_message
), structured_detailed_hourly_errors AS (
  SELECT
    hour,
    document_namespace,
    document_type,
    document_version,
    structured_hourly_errors.ping_count,
    error_examples.error_count,
    SAFE_DIVIDE(1.0 * error_examples.error_count, ping_count) AS error_ratio,
    error_message,
    sample_payload
  FROM
    structured_hourly_errors
  INNER JOIN
    error_examples USING (hour, document_namespace, document_type, document_version, error_type)
)
SELECT * FROM
  structured_detailed_hourly_errors
