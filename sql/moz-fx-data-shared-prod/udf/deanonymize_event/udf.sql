/*

Rename struct fields in anonymous event tuples to meaningful names.

*/
CREATE OR REPLACE FUNCTION udf.deanonymize_event(
  tuple STRUCT<
    f0_ INT64,
    f1_ STRING,
    f2_ STRING,
    f3_ STRING,
    f4_ STRING,
    f5_ ARRAY<STRUCT<key STRING, value STRING>>
  >
) AS (
  STRUCT(
      -- Bug 1602521; some clients report bogus negative timestamps
    IF(tuple.f0_ < 0, NULL, tuple.f0_) AS event_timestamp,
    tuple.f1_ AS event_category,
    tuple.f2_ AS event_method,
    tuple.f3_ AS event_object,
    tuple.f4_ AS event_string_value,
    tuple.f5_ AS event_map_values
  )
);

-- Tests
SELECT
  mozfun.assert.equals(1, udf.deanonymize_event(event).event_timestamp),
  mozfun.assert.equals("normandy", udf.deanonymize_event(event).event_category),
  mozfun.assert.equals("enroll", udf.deanonymize_event(event).event_method),
  mozfun.assert.equals("pref-flip", udf.deanonymize_event(event).event_object),
  mozfun.assert.equals("test-experiment", udf.deanonymize_event(event).event_string_value),
  mozfun.assert.equals("branch", udf.deanonymize_event(event).event_map_values[OFFSET(0)].key),
  mozfun.assert.equals("control", udf.deanonymize_event(event).event_map_values[OFFSET(0)].value)
FROM
  (
    SELECT
      STRUCT(
        1 AS f0_,
        "normandy" AS f1_,
        "enroll" AS f2_,
        "pref-flip" AS f3_,
        "test-experiment" AS f4_,
        [STRUCT("branch" AS key, "control" AS value)] AS f5_
      ) AS event
  );

SELECT
  mozfun.assert.null(udf.deanonymize_event(event).event_timestamp)
FROM
  (
    SELECT
      STRUCT(
        -859101 AS f0_,
        "normandy" AS f1_,
        "enroll" AS f2_,
        "pref-flip" AS f3_,
        "test-experiment" AS f4_,
        [STRUCT("branch" AS key, "control" AS value)] AS f5_
      ) AS event
  );
