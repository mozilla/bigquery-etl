/*

Returns the number of nanoseconds represented by a Glean timespan struct.

See https://mozilla.github.io/glean/book/user/metrics/timespan.html

*/
CREATE OR REPLACE FUNCTION glean.timespan_nanos(timespan STRUCT<time_unit STRING, value INT64>)
RETURNS INT64 AS (
  CASE
    timespan.time_unit
  WHEN
    'nanosecond'
  THEN
    timespan.value
  WHEN
    'microsecond'
  THEN
    timespan.value * 1000
  WHEN
    'millisecond'
  THEN
    timespan.value * 1000 * 1000
  WHEN
    'second'
  THEN
    timespan.value * 1000 * 1000 * 1000
  WHEN
    'minute'
  THEN
    timespan.value * 1000 * 1000 * 1000 * 60
  WHEN
    'hour'
  THEN
    timespan.value * 1000 * 1000 * 1000 * 60 * 60
  WHEN
    'day'
  THEN
    timespan.value * 1000 * 1000 * 1000 * 60 * 60 * 24
  END
);

-- Tests
SELECT
  assert_equals(345600000000000, glean.timespan_nanos(STRUCT('day', 4))),
  assert_equals(13, glean.timespan_nanos(STRUCT('nanosecond', 13))),
  assert_null(glean.timespan_nanos(STRUCT('nonexistent_unit', 13)))
