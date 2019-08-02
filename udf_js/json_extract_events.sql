CREATE TEMP FUNCTION
  udf_js_json_extract_events (input STRING)
  RETURNS ARRAY<STRUCT<
  event_process STRING,
  event_timestamp INT64,
  event_category STRING,
  event_object STRING,
  event_method STRING,
  event_string_value STRING,
  event_map_values ARRAY<STRUCT<key STRING, value STRING>>
  >>
  LANGUAGE js AS """
    if (input == null) {
      return null;
    }
    var parsed = JSON.parse(input);
    var result = [];
    for (var process in parsed) {
      for (var event of parsed[process]) {
        var structured = {
          "event_process": process,
          "event_timestamp": event[0],
          "event_category": event[1],
          "event_method": event[2],
          "event_object": event[3],
          "event_string_value": event[4],
          "event_map_values": []
        }
        for (var key in event[5]) {
          structured.event_map_values.push({"key": key, "value": event[5][key]})
        }
        result.push(structured)
      }
    }
    return result;
""";

-- Tests

WITH
  events AS (
    SELECT AS VALUE CONCAT('{',
    '"parent":[',
      '[15872099,"uptake.remotecontent.result","uptake","remotesettings","success",',
        '{"age":"6879","source":"settings-changes-monitoring","trigger":"startup"}],',
      '[15872100,"ui","click","back"]], ',
    '"content": [',
      '[15872110,"ui","click","forward", null, {"enabled": "true"}]]}')),
    --
    extracted AS (
      SELECT
        udf_js_json_extract_events(events) as e
      FROM
        events )
    --
    SELECT
      assert_equals('parent', e[OFFSET(0)].event_process),
      assert_equals(15872099, e[OFFSET(0)].event_timestamp),
      assert_equals('uptake.remotecontent.result', e[OFFSET(0)].event_category),
      assert_equals('uptake', e[OFFSET(0)].event_method),
      assert_equals('remotesettings', e[OFFSET(0)].event_object),
      assert_equals('success', e[OFFSET(0)].event_string_value),
      assert_array_equals(
        [
          STRUCT('age' AS key,'6879' AS value),
          STRUCT('source' AS key,'settings-changes-monitoring' AS value),
          STRUCT('trigger' AS key,'startup' AS value)
        ],
        e[OFFSET(0)].event_map_values),
      assert_equals('parent', e[OFFSET(1)].event_process),
      assert_equals(15872100, e[OFFSET(1)].event_timestamp),
      assert_equals('ui', e[OFFSET(1)].event_category),
      assert_equals('click', e[OFFSET(1)].event_method),
      assert_equals('back', e[OFFSET(1)].event_object),
      assert_null(e[OFFSET(1)].event_string_value),
      assert_array_empty(e[OFFSET(1)].event_map_values),
      assert_equals('content', e[OFFSET(2)].event_process),
      assert_equals(15872110, e[OFFSET(2)].event_timestamp),
      assert_equals('ui', e[OFFSET(2)].event_category),
      assert_equals('click', e[OFFSET(2)].event_method),
      assert_equals('forward', e[OFFSET(2)].event_object),
      assert_null(e[OFFSET(2)].event_string_value),
      assert_array_equals([STRUCT('enabled' AS key,'true' AS value)], e[OFFSET(2)].event_map_values)
  FROM
    extracted
