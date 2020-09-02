-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.json_mode_last(list ANY TYPE) AS (
  mozfun.json.mode_last(list)
);
