DECLARE datasets ARRAY<STRING>;

DECLARE i INT64 DEFAULT 0;

SET datasets = (
  SELECT
    ARRAY_AGG(schema_name)
  FROM
    `moz-fx-data-shared-prod.INFORMATION_SCHEMA.SCHEMATA`
  WHERE
    schema_name LIKE r'%\_stable%'
);

CREATE TEMP TABLE
  columns(dataset STRING, table_name STRING, total_columns INT64);

LOOP
  SET i = i + 1;

  IF
    i > ARRAY_LENGTH(datasets)
  THEN
    LEAVE;
  END IF;

  EXECUTE IMMEDIATE '''
    INSERT columns
    SELECT "''' || datasets[
    ORDINAL(i)
  ] || '''" AS dataset, 
      table_name, 
      COUNT(DISTINCT(field_path)) AS total_columns 
    FROM `moz-fx-data-shared-prod`.''' || datasets[
    ORDINAL(i)
  ] || '''.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    WHERE
      data_type NOT LIKE "%STRUCT%"
      AND data_type NOT LIKE "%ARRAY%"
    GROUP BY
      table_name  
    ''';
END LOOP;

-- Delete existing records for day.
-- Prevents having duplicates when re-running query.
DELETE FROM
  monitoring_derived.stable_table_column_counts_v1
WHERE
  submission_date = DATE(@submission_date);

-- Insert into target table
INSERT INTO
  monitoring_derived.stable_table_column_counts_v1
SELECT
  DATE(@submission_date) AS submission_date,
  dataset,
  table_name,
  total_columns
FROM
  columns
