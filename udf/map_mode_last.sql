-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.map_mode_last(entries ANY TYPE) AS (
  mozfun.map.mode_last(entries)
);
