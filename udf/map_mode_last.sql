CREATE TEMP FUNCTION
  udf_map_mode_last(maps ANY TYPE) AS (STRUCT(ARRAY(
      SELECT
        AS STRUCT
        key,
        udf_mode_last(ARRAY_AGG(value)) AS value
      FROM
        UNNEST(maps),
        UNNEST(key_value)
      GROUP BY
        key) AS key_value));
