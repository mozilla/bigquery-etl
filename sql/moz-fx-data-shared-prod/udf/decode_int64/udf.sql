CREATE OR REPLACE FUNCTION udf.decode_int64(raw BYTES) AS (
  CAST(
      -- bigquery can only decode raw int from bytes via hex
    CONCAT(
      '0x',
      TO_HEX(
          -- remove most significant bit because INT64 is signed
        raw & b"\x7f\xff\xff\xff\xff\xff\xff\xff"
      )
    ) AS INT64
  )
    -- apply sign from most significant bit
  + IF(
      -- if most significant bit is set
    SUBSTR(raw, 1, 1) > b"\x7f",
      -- then apply sign
    -0x8000000000000000,
      -- else sign is already correct
    0
  )
);

-- Tests
SELECT
  mozfun.assert.equals(1, udf.decode_int64(b'\x00\x00\x00\x00\x00\x00\x00\x01')),
  mozfun.assert.equals(16, udf.decode_int64(b'\x00\x00\x00\x00\x00\x00\x00\x10'));
