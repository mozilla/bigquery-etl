WITH payload_bytes_error_all AS (
  -- Direct access to payload_bytes_error is restricted to airflow
  -- Use the tables in the errors dataset for testing
  -- e.g. moz-fx-data-shared-prod.errors.structured_firefox_desktop__metrics_v1
  SELECT
    'structured' AS pipeline_family,
    *
  FROM
    `moz-fx-data-shared-prod.payload_bytes_error.structured`
  UNION ALL
  SELECT
    'stub_installer' AS pipeline_family,
    * REPLACE (NULL AS payload)
  FROM
    `moz-fx-data-shared-prod.payload_bytes_error.stub_installer`
  UNION ALL
  SELECT
    'telemetry' AS pipeline_family,
    * REPLACE (NULL AS payload)
  FROM
    `moz-fx-data-shared-prod.payload_bytes_error.telemetry`
),
extracted AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    job_name,
    document_namespace,
    document_type,
    document_version,
    error_message,
    uri,
    pipeline_family,
    payload,
  FROM
    payload_bytes_error_all
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
    CASE
      pipeline_family
      WHEN 'structured'
        -- this only works for glean-schema pings but glean makes up nearly all structured telemetry
        THEN JSON_VALUE(
            `moz-fx-data-shared-prod.udf_js.gunzip`(payload),
            '$.client_info.app_channel'
          )
      ELSE `moz-fx-data-shared-prod.udf.parse_desktop_telemetry_uri`(uri).app_update_channel
    END AS channel,
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
    path,
    channel
)
SELECT
  @submission_date AS submission_date,
  *
FROM
  count_errors
