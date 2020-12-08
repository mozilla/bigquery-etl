DECLARE datasets ARRAY<STRING>;

DECLARE tables ARRAY<STRING>;

DECLARE i INT64 DEFAULT 0;

DECLARE t INT64 DEFAULT 0;

DECLARE ignored_tables ARRAY<STRING>;

-- tables that should be ignored, e.g. because we don't have permission to access them
SET ignored_tables = ["telemetry_stable.regrets_reporter_update_v4"];

SET datasets = (
  SELECT
    ARRAY_AGG(schema_name)
  FROM
    `moz-fx-data-shared-prod.INFORMATION_SCHEMA.SCHEMATA`
  WHERE
    schema_name LIKE '%_stable%'
);

CREATE TEMP TABLE
  total_pings(dataset_id STRING, table_id STRING, total INT64);

LOOP
  SET i = i + 1;

  IF
    i > ARRAY_LENGTH(datasets)
  THEN
    LEAVE;
  END IF;

  CREATE OR REPLACE TEMP TABLE tables(table_name STRING);

  EXECUTE IMMEDIATE '''
    INSERT tables
    SELECT table_name
    FROM `moz-fx-data-shared-prod`. ''' | |datasets[
    ORDINAL(i)
  ] | |'''.INFORMATION_SCHEMA.TABLES
  ''';

  SET tables = (SELECT ARRAY_AGG(table_name) FROM tables);

  SET t = 0;

  LOOP
    SET t = t + 1;

    IF
      t > ARRAY_LENGTH(tables)
    THEN
      LEAVE;
    END IF;

    IF
      tables[ORDINAL(t)] NOT IN UNNEST(ignored_tables)
    THEN
      EXECUTE IMMEDIATE '''
        INSERT total_pings
        SELECT "''' | |datasets[
        ORDINAL(i)
      ] | |'''" AS dataset_id,
          "''' | |tables[
        ORDINAL(t)
      ] | |'''" AS table_id, 
          COUNT(*) AS total 
        FROM `moz-fx-data-shared-prod`.''' | |datasets[
        ORDINAL(i)
      ] | |'''.''' | |tables[
        ORDINAL(t)
      ] | |'''
        WHERE
          DATE(submission_timestamp) = @submission_date
        ''';
    END IF;
  END LOOP;
END LOOP;

SELECT
  @submission_date AS submission_date,
  dataset_id,
  table_id,
  COALESCE(SAFE_DIVIDE(byte_size, total), 0) AS average_byte_size
FROM
  total_pings
LEFT JOIN
  `moz-fx-data-shared-prod`.monitoring.stable_table_sizes_v1
USING
  (table_id, dataset_id)
WHERE
  submission_date = @submission_date
