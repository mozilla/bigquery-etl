/*
Return the frequency, recency, and T from a BYTE array, as defined in
https://lifetimes.readthedocs.io/en/latest/Quickstart.html#the-shape-of-your-data

RFM refers to Recency, Frequency, and Monetary value.
*/
CREATE OR REPLACE FUNCTION udf.days_seen_bytes_to_rfm(days_seen_bytes BYTES) AS (
  STRUCT(
    udf.bits_to_days_seen(days_seen_bytes) AS frequency,
    udf.bits_to_days_since_first_seen(days_seen_bytes) AS T,
    udf.bits_to_days_since_first_seen(days_seen_bytes) - udf.bits_to_days_since_seen(
      days_seen_bytes
    ) AS recency
  )
);

-- Tests
SELECT
  mozfun.assert.equals(
    STRUCT(2 AS frequency, 4 AS T, 2 AS recency),
    udf.days_seen_bytes_to_rfm(b'\x14')
  )
