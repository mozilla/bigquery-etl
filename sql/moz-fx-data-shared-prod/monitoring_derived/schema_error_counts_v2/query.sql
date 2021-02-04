WITH extracted AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    job_name,
    document_namespace,
    document_type,
    document_version,
    error_message
  FROM
    `moz-fx-data-shared-prod.payload_bytes_error.*`
  WHERE
    date(submission_timestamp) = @submission_date
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
    COUNT(*) AS error_count
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
  @submission_date AS submission_date,
  *
FROM
  count_errors
