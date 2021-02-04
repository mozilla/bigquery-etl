/*

Return an INT64 representing a range of bits from a source bit pattern.

The start_offset must be zero or a negative number indicating an offset from
the rightmost bit in the pattern.

n_bits is the number of bits to consider, counting right from the bit at start_offset.

See detailed docs for the bits28 suite of functions:
https://docs.telemetry.mozilla.org/cookbooks/clients_last_seen_bits.html#udf-reference

*/
CREATE OR REPLACE FUNCTION bits28.range(bits INT64, start_offset INT64, n_bits INT64)
RETURNS INT64 AS (
  CASE
  WHEN
    start_offset > 0
  THEN
    ERROR(
      FORMAT(
        'start_offset must be <= 0 but was %i in call bits28_range(%i, %i, %i)',
        start_offset,
        bits,
        start_offset,
        n_bits
      )
    )
  WHEN
    n_bits > (1 - start_offset)
  THEN
    ERROR(
      FORMAT(
        'Reading %i bits from starting_offset %i exceeds end of bit pattern in call bits28_range(%i, %i, %i)',
        n_bits,
        start_offset,
        bits,
        start_offset,
        n_bits
      )
    )
  ELSE
    bits << (64 + start_offset - 1) >> (64 - n_bits)
  END
);

-- Tests
SELECT
  assert.equals(1 << 3, bits28.range(1 << 10, -13, 7)),
  assert.equals(0, bits28.range(1 << 10, -6, 7)),
  assert.equals(1, bits28.range(1, 0, 1)),
  assert.equals(0, bits28.range(0, 0, 1));
