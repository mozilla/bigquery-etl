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
    (SELECT
      AS STRUCT tuple.f0_ AS `timestamp`,
      tuple.f1_ AS category,
      tuple.f2_ AS method,
      tuple.f3_ AS object,
      tuple.f4_ AS string_value,
      tuple.f5_ AS map_values)
);

-- Tests

SELECT
    assert_equals(1, udf_deanonymize_event(event).`timestamp`),
    assert_equals("normandy", udf_deanonymize_event(event).category),
    assert_equals("enroll", udf_deanonymize_event(event).method),
    assert_equals("pref-flip", udf_deanonymize_event(event).object),
    assert_equals("test-experiment", udf_deanonymize_event(event).string_value),
    assert_equals("branch", udf_deanonymize_event(event).map_values[OFFSET(0)].key),
    assert_equals("control", udf_deanonymize_event(event).map_values[OFFSET(0)].value)
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
