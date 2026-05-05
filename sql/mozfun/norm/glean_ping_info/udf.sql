/*

Accepts a glean ping_info struct as input and returns a modified struct that
includes a few parsed or normalized variants of the input fields.

*/
CREATE OR REPLACE FUNCTION norm.glean_ping_info(ping_info ANY TYPE) AS (
  (
    SELECT AS STRUCT
      ping_info.*,
      mozfun.glean.parse_datetime(ping_info.start_time) AS parsed_start_time,
      mozfun.glean.parse_datetime(ping_info.end_time) AS parsed_end_time
  )
);

-- Tests
SELECT
  assert.equals(
    TIMESTAMP '2019-12-01 09:22:00',
    norm.glean_ping_info(
      STRUCT('2019-12-01T20:22+11:00' AS start_time, '2019-12-01T21:24+11:00' AS end_time)
    ).parsed_start_time
  ),
  assert.equals(
    TIMESTAMP '2019-12-01 10:24:00',
    norm.glean_ping_info(
      STRUCT('2019-12-01T20:22+11:00' AS start_time, '2019-12-01T21:24+11:00' AS end_time)
    ).parsed_end_time
  ),
  assert.null(
    norm.glean_ping_info(
      STRUCT('2019-12-01T20:22+11:00' AS start_time, '2019-12-01T21:24:00+26:00' AS end_time)
    ).parsed_end_time
  ),
  assert.equals(
    TIMESTAMP '2019-12-01 10:24:03.17',
    norm.glean_ping_info(
      STRUCT(
        '2019-12-01T20:22:03.17+11:00' AS start_time,
        '2019-12-01T21:24:03.17+11:00' AS end_time
      )
    ).parsed_end_time
  );
