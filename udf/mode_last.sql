-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.mode_last(list ANY TYPE) AS (
  mozfun.stats.mode_last(list)
);
