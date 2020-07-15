/*

Accepts a pipeline metadata struct as input and returns a modified struct that
includes a few parsed or normalized variants of the input metadata fields.

*/
CREATE OR REPLACE FUNCTION norm.metadata(metadata ANY TYPE) AS (
  (
    SELECT AS STRUCT
      metadata.* REPLACE (
        (
          SELECT AS STRUCT
            metadata.header.*,
            SAFE.PARSE_TIMESTAMP(
              '%a, %d %b %Y %T %Z',
              -- Even though it's not part of the spec, many clients append
              -- '+00:00' to their Date headers, so we strip that suffix.
              REPLACE(metadata.header.`date`, 'GMT+00:00', 'GMT')
            ) AS parsed_date
        ) AS header
      )
  )
);

-- Tests
SELECT
  assert_equals(
    STRUCT(
      STRUCT(
        'Thu, 21 Nov 2019 22:06:06 GMT' AS `date`,
        TIMESTAMP '2019-11-21 22:06:06' AS parsed_date
      ) AS header
    ),
    norm.metadata(STRUCT(STRUCT('Thu, 21 Nov 2019 22:06:06 GMT' AS `date`) AS header))
  ),
  assert_equals(
    TIMESTAMP '2019-11-21 22:06:06',
    norm.metadata(
      STRUCT(STRUCT('Thu, 21 Nov 2019 22:06:06 GMT+00:00' AS `date`) AS header)
    ).header.parsed_date
  ),
  assert_null(
    norm.metadata(
      STRUCT(STRUCT('Thu, 21 Nov 2019 22:06:06 GMT-05:00' AS `date`) AS header)
    ).header.parsed_date
  ),
  assert_null(
    norm.metadata(STRUCT(STRUCT(CAST(NULL AS STRING) AS `date`) AS header)).header.parsed_date
  );
