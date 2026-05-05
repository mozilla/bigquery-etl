CREATE OR REPLACE FUNCTION udf.int_to_hex_string(value INT64) AS (
  (
    SELECT
      STRING_AGG(
        SPLIT('0123456789ABCDEF', '')[OFFSET((value >> (nibbles * 4)) & 0xF)],
        ''
        ORDER BY
          nibbles DESC
      )
    FROM
      UNNEST(GENERATE_ARRAY(0, 16)) AS nibbles
  )
);

-- Test
SELECT
  mozfun.assert.equals('F', LTRIM(udf.int_to_hex_string(15), '0')),
  mozfun.assert.equals('08000000000000000', udf.int_to_hex_string(1 << 63)),
  mozfun.assert.equals('10', LTRIM(udf.int_to_hex_string(1 << 4), '0')),
  mozfun.assert.equals('00000000000000020', udf.int_to_hex_string(32)),
  mozfun.assert.equals(CAST(NULL AS STRING), udf.int_to_hex_string(NULL));
