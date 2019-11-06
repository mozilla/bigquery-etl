/*

Combine entries from multiple maps, determine the value for each key using
udf_mode_last.

*/
CREATE TEMP FUNCTION udf_map_mode_last(entries ANY TYPE) AS (
  ARRAY(
    SELECT AS STRUCT
      key,
      udf_mode_last(ARRAY_AGG(value)) AS value
    FROM
      UNNEST(entries)
    GROUP BY
      key
  )
);
