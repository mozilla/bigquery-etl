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

/*
Validate using openssl

echo -n "message" | openssl dgst -sha256 -hmac "key"

| key | message | result |
|-----------|-------------|----------|
| abc | abc | 2f02e24ae2e1fe880399f27600afa88364e6062bf9bbe114b32fa8f23d03608a |
| abc | 2571674a93584c2ba2b188b757e2fc54 | 8d9729fefc9b9ecc7bd172ae02af84e782c52c29a964331b22ccee6814e97e2d |
| printf 'abc%.0s' {1..100} | abc | bb40d8823bad32fd79d84b6f82d13655942a6efe6b0c005017f44a21d42eff6d |
| printf 'abc%.0s' {1..100} | 2571674a93584c2ba2b188b757e2fc54 | 7e0bcffb2aa98de768bcd5c4f7ee6b88127ee7bc3e45a0335c3d4079f7acf7f9 |
*/
WITH messages AS (
  SELECT
    "abc" AS key,
    "abc" AS message,
    "2f02e24ae2e1fe880399f27600afa88364e6062bf9bbe114b32fa8f23d03608a" AS expected
  UNION ALL
  SELECT
    "abc",
    "2571674a93584c2ba2b188b757e2fc54",
    "8d9729fefc9b9ecc7bd172ae02af84e782c52c29a964331b22ccee6814e97e2d"
  UNION ALL
  SELECT
    REPEAT("abc", 100),
    "abc",
    "007498569717004c553666f9e1c3d9282b5bf084b50b78f73a96fa1d17f32d08"
  UNION ALL
  SELECT
    REPEAT("abc", 100),
    "2571674a93584c2ba2b188b757e2fc54",
    "a89804addfd5bb1e6b98ab4e9432db508516047b670ede60b6b71aa4d80105e2"
)
SELECT
  assert_equals(expected, TO_HEX(udf.hmac_sha256(CAST(key AS BYTES), CAST(message AS BYTES))))
FROM
  messages
