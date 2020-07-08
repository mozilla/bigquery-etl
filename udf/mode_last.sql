-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.mode_last(list ANY TYPE) AS (
  mozfun.mode.last(list ANY TYPE)
);
