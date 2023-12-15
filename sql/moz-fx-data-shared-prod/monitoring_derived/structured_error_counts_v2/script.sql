DECLARE dummy INT64; -- dummy variable to indicate to bigquery-etl that this is a script
CREATE TEMP TABLE
  ping_counts(
    submission_date DATE,
    document_namespace STRING,
    document_type STRING,
    document_version STRING,
    ping_count INT64
  );

FOR record IN (
  SELECT
    schema_name AS dataset_id
  FROM
    `moz-fx-data-shared-prod.INFORMATION_SCHEMA.SCHEMATA`
  WHERE
    schema_name LIKE "%_live%"
)
DO
  EXECUTE IMMEDIATE CONCAT(
    "INSERT ping_counts (submission_date, document_namespace, document_type, document_version, ping_count) ",
    "SELECT PARSE_DATE('%Y%m%d', PARTITION_ID) AS submission_date, ",
    "REPLACE(TABLE_SCHEMA, '_live', '') AS document_namespace, ",
    "REGEXP_EXTRACT(TABLE_NAME, r'(.+)_v[0-9]+') AS document_type, ",
    "REGEXP_EXTRACT(TABLE_NAME, r'.+_v([0-9]+)') AS document_version, ",
    "TOTAL_ROWS AS ping_count ",
    "FROM ",
    record.dataset_id,
    ".INFORMATION_SCHEMA.PARTITIONS ",
    "WHERE PARTITION_ID != '__NULL__' AND ",
    "PARSE_DATE('%Y%m%d', PARTITION_ID) < CURRENT_DATE AND ('",
    @submission_date,
    "' IS NULL OR '",
    @submission_date,
    "' = PARSE_DATE('%Y%m%d', PARTITION_ID))"
  );
END
FOR;

CREATE TEMP TABLE
  error_counts(
    submission_date DATE,
    document_namespace STRING,
    document_type STRING,
    document_version STRING,
    ping_count INTEGER,
    error_type STRING,
    error_count INTEGER,
    error_ratio FLOAT64
  )
AS
WITH errors AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    document_namespace,
    document_type,
    document_version,
    error_type,
    COUNT(*) AS error_count
  FROM
    `moz-fx-data-shared-prod.monitoring.payload_bytes_error_structured`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    document_namespace,
    document_type,
    document_version,
    error_type
)
SELECT
  submission_date,
  document_namespace,
  document_type,
  document_version,
  COALESCE(ping_count, 0) + COALESCE(error_count, 0) AS ping_count,
  error_type,
  COALESCE(error_count, 0) AS error_count,
  SAFE_DIVIDE(
    1.0 * COALESCE(error_count, 0),
    COALESCE(ping_count, 0) + COALESCE(error_count, 0)
  ) AS error_ratio
FROM
  ping_counts
FULL OUTER JOIN
  errors
USING
  (submission_date, document_namespace, document_type, document_version);

MERGE
  `moz-fx-data-shared-prod.monitoring_derived.structured_error_counts_v2` r
USING
  error_counts d
ON
  d.submission_date = r.submission_date
  AND r.document_namespace = d.document_namespace
  AND r.document_type = d.document_type
  AND r.document_version = d.document_version
WHEN NOT MATCHED
THEN
  INSERT
    (
      submission_date,
      document_namespace,
      document_type,
      document_version,
      ping_count,
      error_type,
      error_count,
      error_ratio
    )
  VALUES
    (
      d.submission_date,
      d.document_namespace,
      d.document_type,
      d.document_version,
      d.ping_count,
      d.error_type,
      d.error_count,
      d.error_ratio
    )
  WHEN NOT MATCHED BY SOURCE
    AND r.submission_date = @submission_date
THEN
  DELETE;
