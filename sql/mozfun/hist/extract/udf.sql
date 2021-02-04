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
      json.extract_int_map(JSON_EXTRACT(input, '$.values')) AS `values`
    )
  WHEN
    ARRAY_LENGTH(SPLIT(input, ';')) = 5
  THEN
    -- Input is a compactly encoded boolean histogram like "3;2;5;1,2;0:0,1:5,2:0"
    STRUCT(
      CAST(SPLIT(input, ';')[SAFE_OFFSET(0)] AS INT64) AS bucket_count,
      CAST(SPLIT(input, ';')[SAFE_OFFSET(1)] AS INT64) AS histogram_type,
      CAST(SPLIT(input, ';')[SAFE_OFFSET(2)] AS INT64) AS `sum`,
      ARRAY(
        SELECT
          CAST(bound AS INT64)
        FROM
          UNNEST(SPLIT(SPLIT(input, ';')[SAFE_OFFSET(3)], ',')) AS bound
      ) AS `range`,
      ARRAY(
        SELECT
          STRUCT(
            CAST(SPLIT(entry, ':')[SAFE_OFFSET(0)] AS INT64) AS key,
            CAST(SPLIT(entry, ':')[SAFE_OFFSET(1)] AS INT64) AS value
          )
        FROM
          UNNEST(SPLIT(SPLIT(input, ';')[SAFE_OFFSET(4)], ',')) AS entry
        WHERE
          LENGTH(entry) >= 3
      ) AS `values`
    )
  WHEN
    ARRAY_LENGTH(SPLIT(input, ',')) = 2
  THEN
    -- Input is a compactly encoded boolean histogram like "0,5"
    STRUCT(
      3 AS bucket_count,
      2 AS histogram_type,
      CAST(SPLIT(input, ',')[SAFE_OFFSET(1)] AS INT64) AS `sum`,
      [1, 2] AS `range`,
      [
        STRUCT(0 AS key, CAST(SPLIT(input, ',')[SAFE_OFFSET(0)] AS INT64) AS value),
        STRUCT(1 AS key, CAST(SPLIT(input, ',')[SAFE_OFFSET(1)] AS INT64) AS value),
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
      CAST(SPLIT(input, ',')[SAFE_OFFSET(0)] AS INT64) AS `sum`,
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
  assert.equals(10, bucket_count),
  assert.equals(1, histogram_type),
  assert.equals(2628, `sum`),
  assert.array_equals([1, 100], `range`),
  assert.array_equals(
    [
      STRUCT(0 AS key, 12434 AS value),
      STRUCT(1 AS key, 297 AS value),
      STRUCT(13 AS key, 8 AS value)
    ],
    `values`
  )
FROM
  extracted;

-- We test a histogram with a small values array along with a null histogram;
-- when there is at least one null row, something about the behavior changes such
-- that use of unsafe OFFSET indexing can raise errors, even when contained under
-- a conditional that ensures a large enough array.
WITH histogram AS (
  SELECT
    '{"bucket_count":3,"histogram_type":4,"sum":1,"range":[1,2],"values":{"0":1,"1":0}}' AS h
  UNION ALL
  SELECT
    CAST(NULL AS STRING) AS h
),
--
extracted AS (
  SELECT
    hist.extract(h).*
  FROM
    histogram
)
--
SELECT
  assert.equals(3, bucket_count),
  assert.equals(4, histogram_type),
  assert.equals(1, `sum`),
  assert.array_equals([1, 2], `range`),
  assert.array_equals([STRUCT(0 AS key, 1 AS value), STRUCT(1 AS key, 0 AS value)], `values`)
FROM
  extracted
WHERE
  bucket_count IS NOT NULL;

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
  assert.equals(3, bucket_count),
  assert.equals(2, histogram_type),
  assert.equals(31, `sum`),
  assert.array_equals([1, 2], `range`),
  assert.array_equals(
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
  assert.equals(3, bucket_count),
  assert.equals(4, histogram_type),
  assert.equals(37, `sum`),
  assert.array_equals([1, 2], `range`),
  assert.array_equals([STRUCT(0 AS key, 37 AS value), STRUCT(1 AS key, 0 AS value)], `values`)
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
  assert.equals(3, bucket_count),
  assert.equals(2, histogram_type),
  assert.equals(5, `sum`),
  assert.array_equals([1, 2], `range`),
  assert.array_equals(
    [STRUCT(0 AS key, 0 AS value), STRUCT(1 AS key, 5 AS value), STRUCT(2 AS key, 0 AS value)],
    `values`
  )
FROM
  extracted;

WITH histogram AS (
  SELECT AS VALUE
    '51;1;0;1,50;'
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
  assert.equals(51, bucket_count),
  assert.equals(1, histogram_type),
  assert.equals(0, `sum`),
  assert.array_equals([1, 50], `range`),
  assert.array_empty(`values`)
FROM
  extracted;
