/*

Return a path derived from an error message in `payload_bytes_error`

 */
CREATE OR REPLACE FUNCTION udf.extract_schema_validation_path(error_message STRING)
RETURNS STRING AS (
  IF(
    STARTS_WITH(error_message, "org.everit.json.schema.ValidationException"),
    TRIM(SPLIT(error_message, ":")[OFFSET(1)]),
    NULL
  )
);

-- Tests
SELECT
  assert.null(
    udf.extract_schema_validation_path(
      "com.mozilla.telemetry.decoder.Deduplicate$DuplicateIdException: A message with this documentId has already been successfully processed."
    )
  ),
  assert.equals(
    "#/events/1/timestamp",
    udf.extract_schema_validation_path(
      "org.everit.json.schema.ValidationException: #/events/1/timestamp: -2 is not greater or equal to 0"
    )
  );
