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
