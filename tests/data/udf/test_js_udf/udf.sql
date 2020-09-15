CREATE OR REPLACE FUNCTION udf.test_js_udf(input BYTES)
RETURNS STRING DETERMINISTIC
LANGUAGE js
AS
  """return 1;"""
OPTIONS
  (library = "gs://path/script.js");
