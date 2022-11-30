CREATE OR REPLACE FUNCTION glean.parse_datetime(datetime_string STRING)
RETURNS TIMESTAMP AS (
  COALESCE(
    SAFE.PARSE_TIMESTAMP('%FT%H:%M:%E*S%Ez', datetime_string),
    SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', datetime_string),
    SAFE.PARSE_TIMESTAMP('%FT%H%Ez', datetime_string),
    SAFE.PARSE_TIMESTAMP('%F%Ez', datetime_string)
  )
);

-- Tests
SELECT
  assert.equals(
    TIMESTAMP '2000-01-02 03:04:05.123456',
    glean.parse_datetime('2000-01-02T03:04:05.123456789+00:00')
  ),
  assert.equals(
    TIMESTAMP '2000-01-02 03:04:05.123456',
    glean.parse_datetime('2000-01-02T03:04:05.123456+00:00')
  ),
  assert.equals(
    TIMESTAMP '2000-01-02 03:04:05.123',
    glean.parse_datetime('2000-01-02T03:04:05.123+00:00')
  ),
  assert.equals(TIMESTAMP '2000-01-02 03:04:05', glean.parse_datetime('2000-01-02T03:04:05+00:00')),
  assert.equals(TIMESTAMP '2000-01-02 09:04:05', glean.parse_datetime('2000-01-02T03:04:05-06:00')),
  assert.equals(TIMESTAMP '2000-01-02 03:04:00', glean.parse_datetime('2000-01-02T03:04+00:00')),
  assert.equals(TIMESTAMP '2000-01-02 03:00:00', glean.parse_datetime('2000-01-02T03+00:00')),
  assert.equals(TIMESTAMP '2000-01-02 00:00:00', glean.parse_datetime('2000-01-02+00:00')),
  assert.null(glean.parse_datetime('2000-01-02 00:00:00 Z'))
