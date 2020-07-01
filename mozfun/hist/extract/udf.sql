CREATE OR REPLACE FUNCTION hist.extract(input STRING) AS (
  CASE
  WHEN
    STARTS_WITH(TRIM(input), '{')
  THEN
    -- Input is a histogram in the classic JSON representation.
    STRUCT(
      CAST(JSON_EXTRACT_SCALAR(input, '$.bucket_count') AS INT64) AS bucket_count,
      CAST(JSON_EXTRACT_SCALAR(input, '$.histogram_type') AS INT64) AS histogram_type,
      CAST(JSON_EXTRACT_SCALAR(input, '$.sum') AS INT64) AS `sum`,
      ARRAY(
        SELECT
          CAST(bound AS INT64)
        FROM
          UNNEST(JSON_EXTRACT_ARRAY(input, '$.range')) AS bound
      ) AS `range`,
      udf.json_extract_int_map(JSON_EXTRACT(input, '$.values')) AS `values`
    )
  WHEN
    ARRAY_LENGTH(SPLIT(input, ';')) = 5
  THEN
    -- Input is a compactly encoded boolean histogram like "3;2;5;1,2;0:0,1:5,2:0"
    STRUCT(
      CAST(SPLIT(input, ';')[OFFSET(0)] AS INT64) AS bucket_count,
      CAST(SPLIT(input, ';')[OFFSET(1)] AS INT64) AS histogram_type,
      CAST(SPLIT(input, ';')[OFFSET(2)] AS INT64) AS `sum`,
      ARRAY(
        SELECT
          CAST(bound AS INT64)
        FROM
          UNNEST(SPLIT(SPLIT(input, ';')[OFFSET(3)], ',')) AS bound
      ) AS `range`,
      ARRAY(
        SELECT
          STRUCT(
            CAST(SPLIT(entry, ':')[OFFSET(0)] AS INT64) AS key,
            CAST(SPLIT(entry, ':')[OFFSET(1)] AS INT64) AS value
          )
        FROM
          UNNEST(SPLIT(SPLIT(input, ';')[OFFSET(4)], ',')) AS entry
      ) AS `values`
    )
  WHEN
    ARRAY_LENGTH(SPLIT(input, ',')) = 2
  THEN
    -- Input is a compactly encoded boolean histogram like "0,5"
    STRUCT(
      3 AS bucket_count,
      2 AS histogram_type,
      CAST(SPLIT(input, ',')[OFFSET(1)] AS INT64) AS `sum`,
      [1, 2] AS `range`,
      [
        STRUCT(0 AS key, CAST(SPLIT(input, ',')[OFFSET(0)] AS INT64) AS value),
        STRUCT(1 AS key, CAST(SPLIT(input, ',')[OFFSET(1)] AS INT64) AS value),
        STRUCT(2 AS key, 0 AS value)
      ] AS `values`
    )
  WHEN
    ARRAY_LENGTH(SPLIT(input, ',')) = 1
  THEN
    -- Input is a compactly encoded count histogram like "5"
    STRUCT(
      3 AS bucket_count,
      4 AS histogram_type,
      CAST(SPLIT(input, ',')[OFFSET(0)] AS INT64) AS `sum`,
      [1, 2] AS `range`,
      [STRUCT(0 AS key, CAST(input AS INT64) AS value), STRUCT(1 AS key, 0 AS value)] AS `values`
    )
  END
);

-- Tests
WITH histogram AS (
  SELECT AS VALUE
    '{"bucket_count":10,"histogram_type":1,"sum":2628,"range":[1,100],"values":{"0":12434,"1":297,"13":8}}'
),
  --
extracted AS (
  SELECT
    hist.extract(histogram).*
  FROM
    histogram
)
  --
SELECT
  assert_equals(10, bucket_count),
  assert_equals(1, histogram_type),
  assert_equals(2628, `sum`),
  assert_array_equals([1, 100], `range`),
  assert_array_equals(
    [
      STRUCT(0 AS key, 12434 AS value),
      STRUCT(1 AS key, 297 AS value),
      STRUCT(13 AS key, 8 AS value)
    ],
    `values`
  )
FROM
  extracted;

WITH histogram AS (
  SELECT AS VALUE
    '0,31'
),
  --
extracted AS (
  SELECT
    hist.extract(histogram).*
  FROM
    histogram
)
  --
SELECT
  assert_equals(3, bucket_count),
  assert_equals(2, histogram_type),
  assert_equals(31, `sum`),
  assert_array_equals([1, 2], `range`),
  assert_array_equals(
    [STRUCT(0 AS key, 0 AS value), STRUCT(1 AS key, 31 AS value), STRUCT(2 AS key, 0 AS value)],
    `values`
  )
FROM
  extracted;

WITH histogram AS (
  SELECT AS VALUE
    '37'
),
--
extracted AS (
  SELECT
    hist.extract(histogram).*
  FROM
    histogram
)
--
SELECT
  assert_equals(3, bucket_count),
  assert_equals(4, histogram_type),
  assert_equals(37, `sum`),
  assert_array_equals([1, 2], `range`),
  assert_array_equals([STRUCT(0 AS key, 37 AS value), STRUCT(1 AS key, 0 AS value)], `values`)
FROM
  extracted;

WITH histogram AS (
  SELECT AS VALUE
    '3;2;5;1,2;0:0,1:5,2:0'
),
  --
extracted AS (
  SELECT
    hist.extract(histogram).*
  FROM
    histogram
)
  --
SELECT
  assert_equals(3, bucket_count),
  assert_equals(2, histogram_type),
  assert_equals(5, `sum`),
  assert_array_equals([1, 2], `range`),
  assert_array_equals(
    [STRUCT(0 AS key, 0 AS value), STRUCT(1 AS key, 5 AS value), STRUCT(2 AS key, 0 AS value)],
    `values`
  )
FROM
  extracted;
