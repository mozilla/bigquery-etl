-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.bits28_to_dates(bits INT64, submission_date DATE)
RETURNS ARRAY<DATE> AS (
  mozfun.bits28.to_dates(bits, submission_date)
);

-- Tests
SELECT
  mozfun.assert.array_empty(udf.bits28_to_dates(0, '2020-01-28')),
  mozfun.assert.array_equals([DATE('2020-01-28')], udf.bits28_to_dates(1, '2020-01-28')),
  mozfun.assert.array_equals(
    [DATE('2020-01-01'), DATE('2020-01-28')],
    udf.bits28_to_dates(1 | (1 << 27), '2020-01-28')
  ),
