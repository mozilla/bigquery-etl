WITH extracted AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    job_name,
    document_namespace,
    document_type,
    document_version,
    error_message
  FROM
    monitoring.payload_bytes_error_all
  WHERE
    DATE(submission_timestamp) = @submission_date
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
    -- aggregating distinct error messages to show sample_error messages
    -- removing path and exception_class for better readability
    SUBSTR(
      STRING_AGG(
        DISTINCT REPLACE(
          REPLACE(error_message, "org.everit.json.schema.ValidationException: ", ""),
          CONCAT(`moz-fx-data-shared-prod.udf.extract_schema_validation_path`(error_message), ": "),
          ""
        ),
        "; "
      ),
      0,
      300
    ) AS sample_error_messages
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
