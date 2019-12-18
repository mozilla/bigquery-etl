/*
Return the frequency, recency, and T from a BYTE array
*/

CREATE TEMP FUNCTION
  udf_days_seen_bytes_to_rfm(days_seen_bytes BYTES) AS (
    STRUCT(
      udf_bits_to_days_seen_int(days_seen_bytes) AS frequency,
      udf_bits_to_first_seen_int(days_seen_bytes) AS T,
      udf_bits_to_first_seen_int(days_seen_bytes)
          - udf_bits_to_last_seen_int(days_seen_bytes) AS recency
    ));

-- Tests

SELECT
  assert_equals(STRUCT(2 AS frequency, 4 AS T, 2 AS recency), udf_days_seen_bytes_to_rfm(b'\x14'))
