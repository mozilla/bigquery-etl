/*
Given a key and message, return the HMAC-SHA256 hash.

This algorithm can be found in Wikipedia:
https://en.wikipedia.org/wiki/HMAC#Implementation

This implentation is validated against the NIST test vectors.
See test/validation/hmac_sha256.py for more information.
*/
CREATE OR REPLACE FUNCTION udf.hmac_sha256(key BYTES, message BYTES) AS (
  SHA256(
    CONCAT(
      RPAD(IF(BYTE_LENGTH(key) > 64, SHA256(key), key), 64, b'\x00') ^ REPEAT(b'\x5c', 64),
      SHA256(
        CONCAT(
          RPAD(IF(BYTE_LENGTH(key) > 64, SHA256(key), key), 64, b'\x00') ^ REPEAT(b'\x36', 64),
          message
        )
      )
    )
  )
);
