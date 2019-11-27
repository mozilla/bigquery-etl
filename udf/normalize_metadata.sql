/*

Accepts a pipeline metadata struct as input and returns a modified struct that
includes a few parsed or normalized variants of the input metadata fields.

*/

CREATE TEMP FUNCTION
  udf_normalize_metadata(metadata ANY TYPE) AS ((
    SELECT
      AS STRUCT metadata.* REPLACE ( (
        SELECT
          AS STRUCT metadata.header.*,
            SAFE.PARSE_TIMESTAMP('%a, %d %b %Y %T %Z', metadata.header.`date`) AS parsed_date) AS header )) );

-- Tests

SELECT
assert_equals(
  STRUCT(STRUCT(
    'Thu, 21 Nov 2019 22:06:06 GMT' AS `date`,
    TIMESTAMP '2019-11-21 22:06:06' AS parsed_date) AS header),
  udf_normalize_metadata(
    STRUCT(STRUCT(
      'Thu, 21 Nov 2019 22:06:06 GMT' AS `date`) AS header))),
assert_null(
  udf_normalize_metadata(
    STRUCT(STRUCT(
    'Thu, 21 Nov 2019 22:06:06 GMT+00:00' AS `date`) AS header)).header.parsed_date),
assert_null(
  udf_normalize_metadata(
    STRUCT(STRUCT(
    CAST(NULL AS STRING) AS `date`) AS header)).header.parsed_date);
