/*

Return the position of the rightmost set bit in an INT64 bit pattern.

To determine this position, we take a bitwise AND of the bit pattern and
its complement, then we determine the position of the bit via base-2 logarithm;
see https://stackoverflow.com/a/42747608/1260237

See detailed docs for the bits28 suite of functions:
https://docs.telemetry.mozilla.org/cookbooks/clients_last_seen_bits.html#udf-reference

*/
CREATE OR REPLACE FUNCTION bits28.days_since_seen(bits INT64) AS (
  CAST(SAFE.LOG(bits & -bits, 2) AS INT64)
);

SELECT
  assert.null(bits28.days_since_seen(0)),
  assert.equals(0, bits28.days_since_seen(1)),
  assert.equals(3, bits28.days_since_seen(8)),
  assert.equals(0, bits28.days_since_seen(8 + 1))
