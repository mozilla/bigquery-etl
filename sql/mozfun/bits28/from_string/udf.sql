/*

Convert a string representing individual bits into an INT64.

Implementation based on https://stackoverflow.com/a/51600210/1260237

See detailed docs for the bits28 suite of functions:
https://docs.telemetry.mozilla.org/cookbooks/clients_last_seen_bits.html#udf-reference

*/
CREATE OR REPLACE FUNCTION bits28.from_string(s STRING) AS (
  IF(
    REGEXP_CONTAINS(s, r"^[01]{1,28}$"),
    (
      SELECT
        SUM(CAST(c AS INT64) << (LENGTH(s) - 1 - bit))
      FROM
        UNNEST(SPLIT(s, '')) AS c
        WITH OFFSET bit
    ),
    ERROR(FORMAT("bits28_from_string expects a string of up to 28 0's and 1's but got: %s", s))
  )
);

-- Tests
SELECT
  assert.equals(1, bits28.from_string('1')),
  assert.equals(1, bits28.from_string('01')),
  assert.equals(1, bits28.from_string('0000000000000000000000000001')),
  assert.equals(2, bits28.from_string('10')),
  assert.equals(5, bits28.from_string('101'));
