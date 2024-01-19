CREATE OR REPLACE PROCEDURE
  utils.diff_query_schemas(
    query_a STRING,
    query_b STRING,
    OUT res ARRAY<
      STRUCT<
        i INT64,
        differs BOOL,
        a_col STRING,
        a_data_type STRING,
        b_col STRING,
        b_data_type STRING
      >
    >
  )
BEGIN
  DECLARE id STRING DEFAULT REPLACE(GENERATE_UUID(), "-", "");

  DECLARE table_a STRING DEFAULT "diff_queries_" || id || "_a";

  DECLARE table_b STRING DEFAULT "diff_queries_" || id || "_b";

  DECLARE table_a_id STRING DEFAULT "mozdata.tmp." || table_a;

  DECLARE table_b_id STRING DEFAULT "mozdata.tmp." || table_b;

  DECLARE create_table_a STRING DEFAULT "CREATE OR REPLACE TABLE " || table_a_id || " AS WITH query AS (" || query_a || ") SELECT * FROM query WHERE FALSE";

  DECLARE create_table_b STRING DEFAULT "CREATE OR REPLACE TABLE " || table_b_id || " AS WITH query AS (" || query_b || ") SELECT * FROM query WHERE FALSE";

  DECLARE query_a_schema ARRAY<
    STRUCT<COLUMN_NAME STRING, ORDINAL_POSITION INT64, DATA_TYPE STRING>
  >;

  DECLARE query_b_schema ARRAY<
    STRUCT<COLUMN_NAME STRING, ORDINAL_POSITION INT64, DATA_TYPE STRING>
  >;

  EXECUTE IMMEDIATE create_table_a;

  EXECUTE IMMEDIATE create_table_b;

  EXECUTE IMMEDIATE "SELECT ARRAY_AGG(STRUCT(COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE)) FROM mozdata.tmp.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @table_name" INTO query_a_schema
  USING table_a AS table_name;

  EXECUTE IMMEDIATE "SELECT ARRAY_AGG(STRUCT(COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE)) FROM mozdata.tmp.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @table_name" INTO query_b_schema
  USING table_b AS table_name;

  SET res = (
    SELECT
      ARRAY_AGG(
        STRUCT(
          ORDINAL_POSITION AS i,
          COALESCE(a.COLUMN_NAME != b.COLUMN_NAME OR a.DATA_TYPE != b.DATA_TYPE, TRUE) AS differs,
          a.COLUMN_NAME AS a_col,
          a.DATA_TYPE AS a_data_type,
          b.COLUMN_NAME AS b_col,
          b.DATA_TYPE AS b_data_type
        )
        ORDER BY
          ORDINAL_POSITION
      )
    FROM
      UNNEST(query_a_schema) AS a
    FULL OUTER JOIN
      (SELECT * FROM UNNEST(query_b_schema)) AS b
      USING (ORDINAL_POSITION)
  );

  -- Cleanup
  EXECUTE IMMEDIATE "DROP TABLE " || table_a_id;

  EXECUTE IMMEDIATE "DROP TABLE " || table_b_id;
END;
