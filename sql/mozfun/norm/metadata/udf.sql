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
            ) AS parsed_date,
            ARRAY(
              SELECT
                TRIM(t)
              FROM
                UNNEST(SPLIT(metadata.header.x_source_tags, ',')) t
            ) AS parsed_x_source_tags
        ) AS header
      )
  )
);

-- Tests
WITH example AS (
  SELECT AS VALUE
    norm.metadata(
      STRUCT(
        STRUCT(
          'Thu, 21 Nov 2019 22:06:06 GMT' AS `date`,
          'automation, performance-test' AS x_source_tags
        ) AS header
      )
    )
)
SELECT
  assert.equals('Thu, 21 Nov 2019 22:06:06 GMT', example.header.`date`),
  assert.equals(TIMESTAMP '2019-11-21 22:06:06', example.header.parsed_date),
  assert.equals('automation, performance-test', example.header.x_source_tags),
  assert.array_equals(['automation', 'performance-test'], example.header.parsed_x_source_tags)
FROM
  example;

--
SELECT
  assert.null(
    norm.metadata(
      STRUCT(
        STRUCT(
          'Thu, 21 Nov 2019 22:06:06 GMT-05:00' AS `date`,
          CAST(NULL AS STRING) AS x_source_tags
        ) AS header
      )
    ).header.parsed_date
  ),
  assert.null(
    norm.metadata(
      STRUCT(
        STRUCT(CAST(NULL AS STRING) AS `date`, CAST(NULL AS STRING) AS x_source_tags) AS header
      )
    ).header.parsed_date
  ),
  assert.array_empty(
    norm.metadata(
      STRUCT(
        STRUCT(CAST(NULL AS STRING) AS `date`, CAST(NULL AS STRING) AS x_source_tags) AS header
      )
    ).header.parsed_x_source_tags
  );
