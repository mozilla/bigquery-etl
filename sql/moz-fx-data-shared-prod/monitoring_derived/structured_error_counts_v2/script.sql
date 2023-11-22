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

MERGE
  `moz-fx-data-shared-prod.monitoring_derived.structured_error_counts_v2` r
USING
  ping_counts d
ON
  d.submission_date = r.submission_date
  AND r.document_namespace = d.document_namespace
  AND r.document_type = d.document_type
  AND r.document_version = d.document_version
WHEN NOT MATCHED
THEN
  INSERT
    (submission_date, document_namespace, document_type, document_version, ping_count)
  VALUES
    (d.submission_date, d.document_namespace, d.document_type, d.document_version, d.ping_count)
  WHEN NOT MATCHED BY SOURCE
THEN
  DELETE;
