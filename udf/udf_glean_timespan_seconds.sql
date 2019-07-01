/*

Returns the number of seconds represented by a Glean timespan struct,
rounded down to full seconds.

See https://mozilla.github.io/glean/book/user/metrics/timespan.html

*/

CREATE TEMP FUNCTION
  udf_glean_timespan_seconds(timespan STRUCT<time_unit STRING, value INT64>)
  RETURNS INT64 AS (
    CAST(
      FLOOR(
        CASE timespan.time_unit
          WHEN 'nanosecond' THEN timespan.value / 1000 / 1000 / 1000
          WHEN 'microsecond' THEN timespan.value / 1000 / 1000
          WHEN 'millisecond' THEN timespan.value / 1000
          WHEN 'second' THEN timespan.value
          WHEN 'minute' THEN timespan.value * 60
          WHEN 'hour' THEN timespan.value * 60 * 60
          WHEN 'day' THEN timespan.value * 60 * 60 * 24
        END )
      AS INT64));

-- Tests

SELECT
  assert_equals(345600, udf_glean_timespan_seconds(STRUCT('day', 4))),
  assert_equals(0, udf_glean_timespan_seconds(STRUCT('nanosecond', 13))),
  assert_equals(13, udf_glean_timespan_seconds(STRUCT('second', 13))),
  assert_null(udf_glean_timespan_seconds(STRUCT('nonexistent_unit', 13)))
