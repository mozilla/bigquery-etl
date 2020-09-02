-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.map_sum(entries ANY TYPE) AS (
  mozfun.map.sum(entries)
);
