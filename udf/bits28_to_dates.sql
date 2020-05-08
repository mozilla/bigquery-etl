/*

Convert a bit pattern into an array of the dates is represents.

See detailed docs for the bits28 suite of functions:
https://docs.telemetry.mozilla.org/cookbooks/clients_last_seen_bits.html#udf-reference

*/
CREATE OR REPLACE FUNCTION udf.bits28_to_dates(bits INT64, submission_date DATE)
RETURNS ARRAY<DATE> AS (
  ARRAY(
    SELECT
      DATE_SUB(submission_date, INTERVAL bit DAY) AS active_date
    FROM
      UNNEST(GENERATE_ARRAY(0, 63)) AS bit
    WHERE
      (bits >> bit & 0x1) = 0x1
    ORDER BY
      active_date
  )
);

-- Tests
SELECT
  assert_array_empty(udf.bits28_to_dates(0, '2020-01-28')),
  assert_array_equals([DATE('2020-01-28')], udf.bits28_to_dates(1, '2020-01-28')),
  assert_array_equals(
    [DATE('2020-01-01'), DATE('2020-01-28')],
    udf.bits28_to_dates(1 | (1 << 27), '2020-01-28')
  ),
