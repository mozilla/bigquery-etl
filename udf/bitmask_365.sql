-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.bitmask_365() AS (
  mozfun.bitmask.bitmask_365()
);
