CREATE OR REPLACE FUNCTION udf.bits_from_offsets(offsets ARRAY<INT64>) AS (
  (
    SELECT
      LTRIM(
        STRING_AGG(
          (
            SELECT
              -- CODE_POINTS_TO_BYTES is the best available interface for converting
              -- an array of numeric values to a BYTES field; it requires that values
              -- are valid extended ASCII characters, so we can only aggregate 8 bits
              -- at a time, and then concat all those chunks together with STRING_AGG.
              CODE_POINTS_TO_BYTES(
                [
                  BIT_OR(
                    (
                      IF(
                        DIV(n - (8 * i), 8) = 0
                        AND (n - (8 * i)) >= 0,
                        1 << MOD(n - (8 * i), 8),
                        0
                      )
                    )
                  )
                ]
              )
            FROM
              UNNEST(offsets) AS n
          ),
          b''
        ),
        b'\x00'
      )
    FROM
      -- Each iteration handles 8 bits, so 256 iterations gives us 2048 bits,
      -- about 5.6 years worth. This function performs an LTRIM, so additional
      -- bits are dropped and it's safe to expand this range arbitrarily.
      UNNEST(GENERATE_ARRAY(255, 0, -1)) AS i
  )
);

-- Tests
SELECT
  mozfun.assert.equals(b'\x01', udf.bits_from_offsets([0])),
  mozfun.assert.equals(b'\x02', udf.bits_from_offsets([1])),
  mozfun.assert.equals(b'\x03', udf.bits_from_offsets([0, 1])),
  mozfun.assert.equals(b'\xff', udf.bits_from_offsets([0, 1, 2, 3, 4, 5, 6, 7])),
  mozfun.assert.equals(b'\x01\xff', udf.bits_from_offsets([0, 1, 2, 3, 4, 5, 6, 7, 8])),
  mozfun.assert.equals(b'\x01\xff', udf.bits_from_offsets([8, 0, 1, 2, 3, 4, 5, 6, 7])),
  mozfun.assert.equals(b'\x01', udf.bits_from_offsets([0, 2048])),
  mozfun.assert.equals(CONCAT(b'\x80', REPEAT(b'\x00', 255)), udf.bits_from_offsets([2047]));
