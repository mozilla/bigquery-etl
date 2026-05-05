/*

Return a path derived from an error message in `payload_bytes_error`

 */
CREATE OR REPLACE FUNCTION udf.extract_schema_validation_path(error_message STRING)
RETURNS STRING AS (
  IF(
    STARTS_WITH(error_message, "org.everit.json.schema.ValidationException"),
    CONCAT(
      TRIM(SPLIT(error_message, ":")[OFFSET(1)]),
      COALESCE(
        CONCAT("/", REGEXP_SUBSTR(error_message, r"(?:extraneous|required) key \[([^\]]+)\]")),
        ""
      )
    ),
    NULL
  )
);

-- Tests
SELECT
  mozfun.assert.null(
    udf.extract_schema_validation_path(
      "com.mozilla.telemetry.decoder.Deduplicate$DuplicateIdException: A message with this documentId has already been successfully processed."
    )
  ),
  mozfun.assert.equals(
    "#/events/1/timestamp",
    udf.extract_schema_validation_path(
      "org.everit.json.schema.ValidationException: #/events/1/timestamp: -2 is not greater or equal to 0"
    )
  ),
  mozfun.assert.equals(
    "#/client_info",
    udf.extract_schema_validation_path(
      "org.everit.json.schema.ValidationException: #: required key [client_info] not found"
    )
  ),
  mozfun.assert.equals(
    "#/application/buildID",
    udf.extract_schema_validation_path(
      "org.everit.json.schema.ValidationException: #/application: extraneous key [buildID] is not permitted"
    )
  );
