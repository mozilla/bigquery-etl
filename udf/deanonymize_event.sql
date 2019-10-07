/*

Rename struct fields in anonymous event tuples to meaningful names

*/
CREATE TEMP FUNCTION
  udf_deanonymize_event(tuple STRUCT<
    f0_ INT64,
    f1_ STRING,
    f2_ STRING,
    f3_ STRING,
    f4_ STRING,
    f5_ ARRAY<STRUCT<key STRING, value STRING>>>) AS (

    STRUCT(tuple.f0_ AS event_timestamp,
      tuple.f1_ AS event_category,
      tuple.f2_ AS event_method,
      tuple.f3_ AS event_object,
      tuple.f4_ AS event_string_value,
      tuple.f5_ AS event_map_values)
);

-- Tests

SELECT
    assert_equals(1, udf_deanonymize_event(event).event_timestamp),
    assert_equals("normandy", udf_deanonymize_event(event).event_category),
    assert_equals("enroll", udf_deanonymize_event(event).event_method),
    assert_equals("pref-flip", udf_deanonymize_event(event).event_object),
    assert_equals("test-experiment", udf_deanonymize_event(event).event_string_value),
    assert_equals("branch", udf_deanonymize_event(event).event_map_values[OFFSET(0)].key),
    assert_equals("control", udf_deanonymize_event(event).event_map_values[OFFSET(0)].value)
FROM (
  SELECT STRUCT(
    1 AS f0_,
    "normandy" AS f1_,
    "enroll" AS f2_,
    "pref-flip" AS f3_,
    "test-experiment" AS f4_,
    [STRUCT("branch" AS key,
    "control" AS value)] AS f5_
   ) AS event
);
