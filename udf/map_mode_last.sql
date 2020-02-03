/*

Combine entries from multiple maps, determine the value for each key using
udf.mode_last.

*/
CREATE OR REPLACE FUNCTION udf.map_mode_last(entries ANY TYPE) AS (
  ARRAY(
    SELECT AS STRUCT
      key,
      udf.mode_last(ARRAY_AGG(value)) AS value
    FROM
      UNNEST(entries)
    GROUP BY
      key
  )
);
