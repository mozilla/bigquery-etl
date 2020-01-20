/*

Extract all parts of the `uri` field used in payload tables. 

*/
CREATE TEMP FUNCTION udf_extract_payload_uri(uri STRING)
RETURNS STRUCT<
  prefix STRING,
  document_namespace STRING,
  document_id STRING,
  document_type STRING,
  browser STRING,
  version STRING,
  channel STRING,
  build_id STRING
> AS (
  (
    SELECT
      STRUCT(
        SPLIT(uri, "/")[OFFSET(1)] AS prefix,
        SPLIT(uri, "/")[OFFSET(2)] AS document_namespace,
        SPLIT(uri, "/")[OFFSET(3)] AS document_id,
        SPLIT(uri, "/")[OFFSET(4)] AS document_type,
        SPLIT(uri, "/")[OFFSET(5)] AS browser,
        SPLIT(uri, "/")[OFFSET(6)] AS version,
        SPLIT(uri, "/")[OFFSET(7)] AS channel,
        SPLIT(uri, "/")[OFFSET(8)] AS build_id
      )
  )
);
-- Tests
SELECT
  assert_equals(
    "submit",
    udf_extract_payload_uri(
      "/submit/telemetry/9ea03c76-1d1e-4a12-b1db-6c93d7b0d052/core/Fennec/46.0.1/release/20160502161457"
    ).prefix
  ),
  assert_equals(
    "telemetry",
    udf_extract_payload_uri(
      "/submit/telemetry/9ea03c76-1d1e-4a12-b1db-6c93d7b0d052/core/Fennec/46.0.1/release/20160502161457"
    ).document_namespace
  ),
  assert_equals(
    "9ea03c76-1d1e-4a12-b1db-6c93d7b0d052",
    udf_extract_payload_uri(
      "/submit/telemetry/9ea03c76-1d1e-4a12-b1db-6c93d7b0d052/core/Fennec/46.0.1/release/20160502161457"
    ).document_id
  ),
  assert_equals(
    "core",
    udf_extract_payload_uri(
      "/submit/telemetry/9ea03c76-1d1e-4a12-b1db-6c93d7b0d052/core/Fennec/46.0.1/release/20160502161457"
    ).document_type
  ),
  assert_equals(
    "Fennec",
    udf_extract_payload_uri(
      "/submit/telemetry/9ea03c76-1d1e-4a12-b1db-6c93d7b0d052/core/Fennec/46.0.1/release/20160502161457"
    ).browser
  ),
  assert_equals(
    "46.0.1",
    udf_extract_payload_uri(
      "/submit/telemetry/9ea03c76-1d1e-4a12-b1db-6c93d7b0d052/core/Fennec/46.0.1/release/20160502161457"
    ).version
  ),
  assert_equals(
    "release",
    udf_extract_payload_uri(
      "/submit/telemetry/9ea03c76-1d1e-4a12-b1db-6c93d7b0d052/core/Fennec/46.0.1/release/20160502161457"
    ).channel
  ),
  assert_equals(
    "20160502161457",
    udf_extract_payload_uri(
      "/submit/telemetry/9ea03c76-1d1e-4a12-b1db-6c93d7b0d052/core/Fennec/46.0.1/release/20160502161457"
    ).build_id
  );
