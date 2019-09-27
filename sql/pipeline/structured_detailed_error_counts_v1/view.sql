CREATE TEMP FUNCTION
  udf_js_gunzip (input BYTES)
  RETURNS STRING
  LANGUAGE js AS """
    /*  Input is either:
     *    - A gzipped UTF-8 byte array
     *    - A UTF-8 byte array
     *
     *  Outputs a string representation
     *  of the byte array (gunzipped if
     *  possible).
     */

    function binary2String(byteArray) {
        // converts a UTF-16 byte array to a string
        return String.fromCharCode.apply(String, byteArray);
    }
    
    // BYTES are base64 encoded by BQ, so this needs to be decoded
    // Outputs a UTF-16 string
    var decodedData = atob(input);

    // convert UTF-16 string to byte array
    var compressedData = decodedData.split('').map(function(e) {
        return e.charCodeAt(0);
    });
    
    try {
      var gunzip = new Zlib.Gunzip(compressedData); 
    
      // decompress returns bytes that need to be converted into a string
      var unzipped = gunzip.decompress();
      return binary2String(unzipped);
    } catch (err) {
      return binary2String(compressedData);
    }
"""
OPTIONS (
  library = "gs://moz-fx-data-circleci-tests-bigquery-etl/gunzip.min.js",
  library = "gs://moz-fx-data-circleci-tests-bigquery-etl/atob.js"
);
--
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.pipeline.detailed_structured_error_counts_v1`
AS
WITH error_examples AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    document_namespace,
    document_type,
    document_version,
    error_type,
    error_message,
    udf_js_gunzip(ARRAY_AGG(payload)[OFFSET(0)]) AS sample_payload,
    COUNT(*) AS error_count
  FROM
    payload_bytes_error.structured
  WHERE
    submission_timestamp >= TIMESTAMP_SUB(current_timestamp, INTERVAL 28 * 24 HOUR)
  GROUP BY hour, document_namespace, document_type, document_version, error_type, error_message
), structured_detailed_hourly_errors AS (
  SELECT
    hour,
    document_namespace,
    document_type,
    document_version,
    structured_hourly_errors.ping_count,
    error_examples.error_count,
    SAFE_DIVIDE(1.0 * error_examples.error_count, ping_count) AS error_ratio,
    error_message,
    sample_payload
  FROM
    structured_hourly_errors
  INNER JOIN
    error_examples USING (hour, document_namespace, document_type, document_version, error_type)
)
SELECT * FROM
  structured_detailed_hourly_errors
