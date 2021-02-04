/*

Convert an INT64 field into a 28-character string representing the individual bits.

Implementation based on https://stackoverflow.com/a/51600210/1260237

See detailed docs for the bits28 suite of functions:
https://docs.telemetry.mozilla.org/cookbooks/clients_last_seen_bits.html#udf-reference

*/
CREATE OR REPLACE FUNCTION bits28.to_string(bits INT64) AS (
  (
    SELECT
      STRING_AGG(CAST(bits >> bit & 0x1 AS STRING), '' ORDER BY bit DESC)
    FROM
      UNNEST(GENERATE_ARRAY(0, 27)) AS bit
  )
);

-- Tests
SELECT
  assert.equals('0100000000000000000000000010', bits28.to_string((1 << 1) | (1 << 26)))
