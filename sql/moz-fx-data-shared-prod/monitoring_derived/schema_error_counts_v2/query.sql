WITH schema_errors AS (
  SELECT
    @submission_date AS submission_date,
    document_namespace,
    document_type,
    document_version,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
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
    ) AS sample_error_messages,
    channel,
    CONCAT(REPLACE(document_namespace, "-", "_"), "_stable") AS dataset_id,
    -- legacy telemetry doesn't get document_version parsed
    CONCAT(REPLACE(document_type, "-", "_"), "_v", COALESCE(document_version, "%")) AS table_id,
  FROM
    `moz-fx-data-shared-prod.monitoring.payload_bytes_error_all`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND exception_class = "org.everit.json.schema.ValidationException"
  GROUP BY
    document_namespace,
    document_type,
    document_version,
    hour,
    job_name,
    path,
    channel
),
table_sizes AS (
  SELECT
    dataset_id,
    -- Sum across doc versions for legacy telemetry because versioning is inconsistent
    IF(
      dataset_id = "telemetry_stable",
      REGEXP_REPLACE(table_id, "_v[0-9]+$", "_v%"),
      table_id
    ) AS table_id,
    SUM(row_count) AS row_count,
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.stable_and_derived_table_sizes_v1`
  WHERE
    submission_date = @submission_date
  GROUP BY
    dataset_id,
    table_id
)
SELECT
  schema_errors.* EXCEPT (dataset_id, table_id),
  table_sizes.row_count AS valid_ping_count,
FROM
  schema_errors
LEFT JOIN
  table_sizes
  USING (dataset_id, table_id)
