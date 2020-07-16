/*

Combine entries from multiple maps, determine the value for each key using
stats.mode_last.

*/
CREATE OR REPLACE FUNCTION map.mode_last(entries ANY TYPE) AS (
  ARRAY(
    SELECT AS STRUCT
      key,
      stats.mode_last(ARRAY_AGG(value)) AS value
    FROM
      UNNEST(entries)
    GROUP BY
      key
  )
);
