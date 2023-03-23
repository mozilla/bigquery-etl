-- pseudonymize_ad_id: Only for use in Ad ID pipeline
CREATE OR REPLACE FUNCTION udf.pseudonymize_ad_id(hashed_ad_id STRING, key BYTES)
RETURNS STRING AS (
  IF( -- These two values represent the hashed all-0 Ad IDs, see bug 1823482
    hashed_ad_id IN (
      "q7RZx9leE4AHjn9DRQR8kLtjO0FNwmj/61ECGxFinno=",
      "ksEEnM6/esBxl2myqrVLo31ePBoQZZnkCm81vefaF90="
    ),
    NULL,
    TO_HEX(udf.hmac_sha256(key, CAST(hashed_ad_id AS BYTES)))
  )
);

-- Tests
SELECT
  assert.null(udf.pseudonymize_ad_id("q7RZx9leE4AHjn9DRQR8kLtjO0FNwmj/61ECGxFinno=", b"\x14")),
  assert.null(udf.pseudonymize_ad_id("ksEEnM6/esBxl2myqrVLo31ePBoQZZnkCm81vefaF90=", b"\x14")),
  assert.not_null(udf.pseudonymize_ad_id("7RZx9leE4AHjn9DRQR8kLtjO0FNwmj/61ECGxFinno=", b"\x14")),
  assert.not_null(udf.pseudonymize_ad_id("sEEnM6/esBxl2myqrVLo31ePBoQZZnkCm81vefaF90=", b"\x14")),
  assert.equals(
    udf.pseudonymize_ad_id("abc", b"\x14"),
    TO_HEX(udf.hmac_sha256(b"\x14", CAST("abc" AS BYTES)))
  )
