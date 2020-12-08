DECLARE tables ARRAY<STRING>;

DECLARE i INT64 DEFAULT 0;

SET tables = (
  SELECT
    ARRAY_AGG(CONCAT(dataset_id, ".", table_id))
  FROM
    `moz-fx-data-shared-prod`.monitoring.stable_table_sizes_v1
);

CREATE TEMP TABLE
  total_pings(table_id STRING, total INT64);

LOOP
  SET i = i + 1;

  IF
    i > ARRAY_LENGTH(tables)
  THEN
    LEAVE;
  END IF;

  EXECUTE IMMEDIATE '''
    INSERT total_pings
    SELECT "''' | |tables[
    ORDINAL(i)
  ] | |'''" AS table_id, 
      COUNT(*) AS total 
    FROM `moz-fx-data-shared-prod`.''' | |tables[
    ORDINAL(i)
  ] | |'''
    WHERE
      DATE(submission_timestamp) = @submission_date
    ''';
END LOOP;

SELECT
  @submission_date AS submission_date,
  dataset_id,
  table_id,
  COALESCE(SAFE_DIVIDE(byte_size, total), 0) AS average_byte_size
FROM
  total_pings t
LEFT JOIN
  `moz-fx-data-shared-prod`.monitoring.stable_table_sizes_v1 s
WHERE
  CONCAT(s.dataset_id, ".", s.table_id) = t.table_id
  AND submission_date = @submission_date
