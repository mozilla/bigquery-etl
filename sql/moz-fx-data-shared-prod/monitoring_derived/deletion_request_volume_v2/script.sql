DECLARE dummy INT64; -- declare a dummy variable to indicate that this is a script to bigquery-etl
CREATE TEMP TABLE
  deletion_counts(submission_date DATE, dataset_id STRING, num_rows INT64);

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
    "INSERT deletion_counts (submission_date, dataset_id, num_rows) ",
    "SELECT PARSE_DATE('%Y%m%d', PARTITION_ID) AS submission_date, REPLACE(TABLE_SCHEMA, '_live', '') AS dataset_id, TOTAL_ROWS AS num_rows ",
    "FROM ",
    record.dataset_id,
    ".INFORMATION_SCHEMA.PARTITIONS ",
    "WHERE TABLE_NAME = 'deletion_request_v1' AND ",
    "PARTITION_ID != '__NULL__' AND ",
    "PARSE_DATE('%Y%m%d', PARTITION_ID) < CURRENT_DATE AND ('",
    @submission_date,
    "' IS NULL OR '",
    @submission_date,
    "' = PARSE_DATE('%Y%m%d', PARTITION_ID))"
  );
END
FOR;

MERGE
  `moz-fx-data-shared-prod.monitoring_derived.deletion_request_volume_v2` r
  USING deletion_counts d
  ON d.submission_date = r.submission_date
  AND r.dataset_id = d.dataset_id
WHEN NOT MATCHED
THEN
  INSERT
    (submission_date, dataset_id, num_rows)
  VALUES
    (d.submission_date, d.dataset_id, num_rows)
  WHEN NOT MATCHED BY SOURCE
    AND r.submission_date = @submission_date
THEN
  DELETE;
