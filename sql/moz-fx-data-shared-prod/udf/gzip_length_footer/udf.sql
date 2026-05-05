/*
Given a gzip compressed byte string, extract the uncompressed size from the footer.

WARNING: THIS FUNCTION IS NOT RELIABLE FOR ARBITRARY GZIP STREAMS. It should,
however, be safe to use for checking the decompressed size of payload in
payload_bytes_decoded (and NOT payload_bytes_raw) because that payload is
produced by the decoder and limited to conditions where the footer is accurate.

From https://stackoverflow.com/a/9213826

    First, the only information about the uncompressed length is four bytes at
    the end of the gzip file (stored in little-endian order). By necessity,
    that is the length modulo 232. So if the uncompressed length is 4 GB or
    more, you won't know what the length is. You can only be certain that the
    uncompressed length is less than 4 GB if the compressed length is less than
    something like 232 / 1032 + 18, or around 4 MB. (1032 is the maximum
    compression factor of deflate.)

    Second, and this is worse, a gzip file may actually be a concatenation of
    multiple gzip streams. Other than decoding, there is no way to find where
    each gzip stream ends in order to look at the four-byte uncompressed length
    of that piece. (Which may be wrong anyway due to the first reason.)

    Third, gzip files will sometimes have junk after the end of the gzip stream
    (usually zeros). Then the last four bytes are not the length.

*/
CREATE OR REPLACE FUNCTION udf.gzip_length_footer(compressed BYTES) AS (
  CAST(
    CONCAT(
      '0x',
      -- bigquery can only decode raw int from bytes via hex
      TO_HEX(
        -- use reverse to convert from little-endian to big-endian
        REVERSE(
          -- uncompressed length is four bytes at the end of the gzip file
          SUBSTR(compressed, -4)
        )
      )
    ) AS INT64
  )
);

-- Tests
SELECT
  mozfun.assert.equals(
    4,
    udf.gzip_length_footer(
      -- printf test | gzip -c | python3 -c 'import sys; print(sys.stdin.buffer.read())'
      b'\x1f\x8b\x08\x00\xc4\x15\x8e^\x00\x03+I-.\x01\x00\x0c~\x7f\xd8\x04\x00\x00\x00'
    )
  ),
  mozfun.assert.equals(
    0,
    udf.gzip_length_footer(
      -- gzip -c < /dev/null | python3 -c 'import sys; print(sys.stdin.buffer.read())'
      b'\x1f\x8b\x08\x00\xe7\x15\x8e^\x00\x03\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    )
  ),
  mozfun.assert.equals(16, udf.gzip_length_footer(b"\x10\x00\x00\x00"));
