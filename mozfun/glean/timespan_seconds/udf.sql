/*

Returns the number of seconds represented by a Glean timespan struct,
rounded down to full seconds.

See https://mozilla.github.io/glean/book/user/metrics/timespan.html

*/
CREATE OR REPLACE FUNCTION glean.timespan_seconds(timespan STRUCT<time_unit STRING, value INT64>)
RETURNS INT64 AS (
  CAST(
    FLOOR(
      CASE
        timespan.time_unit
      WHEN
        'nanosecond'
      THEN
        timespan.value / 1000 / 1000 / 1000
      WHEN
        'microsecond'
      THEN
        timespan.value / 1000 / 1000
      WHEN
        'millisecond'
      THEN
        timespan.value / 1000
      WHEN
        'second'
      THEN
        timespan.value
      WHEN
        'minute'
      THEN
        timespan.value * 60
      WHEN
        'hour'
      THEN
        timespan.value * 60 * 60
      WHEN
        'day'
      THEN
        timespan.value * 60 * 60 * 24
      END
    ) AS INT64
  )
);

-- Tests
SELECT
  assert_equals(345600, glean.timespan_seconds(STRUCT('day', 4))),
  assert_equals(0, glean.timespan_seconds(STRUCT('nanosecond', 13))),
  assert_equals(13, glean.timespan_seconds(STRUCT('second', 13))),
  assert_null(glean.timespan_seconds(STRUCT('nonexistent_unit', 13)))
