/*

Unzips a GZIP string.

This implementation relies on the zlib.js library (https://github.com/imaya/zlib.js) and
the atob function for decoding base64.

*/
CREATE OR REPLACE FUNCTION udf_js.gunzip(input BYTES)
RETURNS STRING DETERMINISTIC
LANGUAGE js
AS
  """
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
OPTIONS
  (
    library = "gs://moz-fx-data-circleci-tests-bigquery-etl/gunzip.min.js",
    library = "gs://moz-fx-data-circleci-tests-bigquery-etl/atob.js"
  );

-- Tests
WITH input AS (
  SELECT
    FROM_BASE64('H4sIAKnBGlwAA6uuBQBDv6ajAgAAAA==') AS test_input,
    '{}' AS expected
  UNION ALL
  SELECT
    CAST('{"hello": "world"}' AS BYTES),
    '{"hello": "world"}'
),
  --
unzipped AS (
  SELECT
    udf_js.gunzip(test_input) AS result,
    expected
  FROM
    input
)
  --
SELECT
  mozfun.assert.equals(expected, result)
FROM
  unzipped
