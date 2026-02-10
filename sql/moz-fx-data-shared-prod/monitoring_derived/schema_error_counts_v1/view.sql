CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring_derived.schema_error_counts_v1`
AS
WITH extracted AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    job_name,
    document_namespace,
    document_type,
    document_version,
    error_message
  FROM
    `moz-fx-data-shared-prod.monitoring.payload_bytes_error_all`
  WHERE
    submission_timestamp < TIMESTAMP_TRUNC(current_timestamp, day)
    AND submission_timestamp > TIMESTAMP_SUB(
      TIMESTAMP_TRUNC(current_timestamp, day),
      INTERVAL(28 * 24) hour
    )
    AND exception_class = 'org.everit.json.schema.ValidationException'
),
count_errors AS (
  SELECT
    document_namespace,
    document_type,
    document_version,
    hour,
    job_name,
    `moz-fx-data-shared-prod.udf.extract_schema_validation_path`(error_message) AS path,
    COUNT(*) AS error_count,
    ROW_NUMBER() OVER (
      PARTITION BY
        hour,
        document_namespace,
        document_type,
        document_version
      ORDER BY
        COUNT(*) DESC
    ) AS error_rank
  FROM
    extracted
  GROUP BY
    document_namespace,
    document_type,
    document_version,
    hour,
    job_name,
    path
)
SELECT
  *
FROM
  count_errors
ORDER BY
  document_namespace,
  document_type,
  error_rank,
  hour
