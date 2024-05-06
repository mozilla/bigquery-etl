CREATE OR REPLACE FUNCTION datetime_util.braze_parse_time(json_str STRING)
RETURNS STRING
LANGUAGE js
AS
  """
  try {
    var json = JSON.parse(json_str);
    return json['$time'];
  } catch (e) {
    return null;
  }
""";

-- test
assert.equals(
  "2024-05-03 16:14:09 UTC",
  mozfun.datetime_util.braze_parse_time("{" $ time ":" 2024 - 05 - 03 16 : 14 : 09.000000 UTC "}")
),
