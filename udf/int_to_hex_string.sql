CREATE TEMP FUNCTION
  udf_int_to_hex_string(value INT64) AS ((
    SELECT STRING_AGG(
        SPLIT('0123456789ABCDEF','')[OFFSET((value >> (nibbles*4)) & 0xF)], ''
        ORDER BY nibbles DESC
    )
    FROM UNNEST(generate_array(0, 16)) AS nibbles
));

-- Test

SELECT
  assert_equals('F', ltrim(udf_int_to_hex_string(15), '0')),
  assert_equals('08000000000000000', udf_int_to_hex_string(1 << 63)),
  assert_equals('10', ltrim(udf_int_to_hex_string(1 << 4), '0')),
  assert_equals('00000000000000020', udf_int_to_hex_string(32)),
  assert_equals(CAST(NULL AS STRING), udf_int_to_hex_string(NULL));
