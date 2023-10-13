/*

Floor a timestamp object to the given minute interval.

 */
CREATE OR REPLACE FUNCTION udf.round_timestamp_to_minute(
  timestamp_expression TIMESTAMP,
  minute INT64
) AS (
  TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(timestamp_expression), minute * 60) * minute * 60)
);

-- Test
SELECT
  mozfun.assert.equals(
    TIMESTAMP '2019-01-01 13:30:00',
    udf.round_timestamp_to_minute(TIMESTAMP '2019-01-01 13:31:11', 5)
  ),
  mozfun.assert.equals(
    TIMESTAMP '2019-05-03 00:30:00',
    udf.round_timestamp_to_minute(TIMESTAMP '2019-05-03 00:34:59', 5)
  ),
  mozfun.assert.equals(
    TIMESTAMP '2019-05-03 00:34:00',
    udf.round_timestamp_to_minute(TIMESTAMP '2019-05-03 00:34:59', 2)
  ),
  mozfun.assert.equals(
    TIMESTAMP '2019-05-03 00:34:00',
    udf.round_timestamp_to_minute(TIMESTAMP '2019-05-03 00:34:59.999999', 2)
  )
