-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.mode_last_retain_nulls(list ANY TYPE) AS (
  mozfun.mode.last_retain_nulls(list ANY TYPE)
);
