/*

Unzips a GZIP string.   

This implementation relies on the zlib.js library (https://github.com/imaya/zlib.js) and 
the atob function for decoding base64.

*/


CREATE TEMP FUNCTION
  udf_js_gunzip (input BYTES)
  RETURNS STRING
  LANGUAGE js AS """
    // converts a byte array to a string
    function binary2String(byteArray) {
        return String.fromCharCode.apply(String, byteArray);
    }
    
    // input is in base64, so it needs to be decoded
    var decodedData = atob(input);
    var compressedData = decodedData.split('').map(function(e) {
        return e.charCodeAt(0);
    });
    
    try {
      var gunzip = new Zlib.Gunzip(compressedData); 
    
      // decompress returns bytes that need to be converted into a string
      var unzipped = gunzip.decompress();
      return binary2String(unzipped);
    } catch (err) {
      return binary2String(input);
    }
"""
OPTIONS (
  library = "gs://moz-fx-data-circleci-tests-bigquery-etl/gunzip.min.js",
  library = "gs://moz-fx-data-circleci-tests-bigquery-etl/atob.js"
);

-- Tests

WITH
  gzipped AS (
    SELECT AS VALUE
      FROM_BASE64('H4sIAKnBGlwAA6uuBQBDv6ajAgAAAA==')),
  --
  unzipped AS (
    SELECT
      udf_js_gunzip(gzipped) AS result
    FROM
      gzipped )
  --
  SELECT
    assert_equals('{}', result)
  FROM
    unzipped
