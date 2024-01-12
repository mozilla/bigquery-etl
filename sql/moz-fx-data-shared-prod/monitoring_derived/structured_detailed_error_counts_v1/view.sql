CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring_derived.structured_detailed_error_counts_v1`
AS
WITH error_examples AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    document_namespace,
    document_type,
    document_version,
    error_type,
    error_message,
    -- As of September 2021, payloads in the structured error table are no longer
    -- accessible to all users, so we inject NULL to keep the interface here.
    -- `moz-fx-data-shared-prod.udf_js.gunzip`(ANY_VALUE(payload)) AS sample_payload,
    CAST(NULL AS STRING) AS sample_payload,
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
    error_type,
    error_message
),
structured_detailed_hourly_errors AS (
  SELECT
    hour,
    document_namespace,
    document_type,
    document_version,
    error_type,
    structured_hourly_errors.ping_count,
    COALESCE(error_examples.error_count, 0) AS error_count,
    error_message,
    sample_payload
  FROM
    `moz-fx-data-shared-prod.monitoring.structured_error_counts` structured_hourly_errors
  FULL OUTER JOIN
    error_examples
    USING (hour, document_namespace, document_type, document_version, error_type)
),
with_ratio AS (
  SELECT
    *,
    SAFE_DIVIDE(1.0 * error_count, ping_count) AS error_ratio
  FROM
    structured_detailed_hourly_errors
)
SELECT
  *
FROM
  with_ratio
