CREATE OR REPLACE FUNCTION hist.string_to_json(input STRING) AS (
  CASE
  WHEN
    STARTS_WITH(TRIM(input), '{')
  THEN
    -- Input is a histogram in the classic JSON representation.
    input
  WHEN
    ARRAY_LENGTH(SPLIT(input, ';')) = 5
  THEN
    -- Input is a compactly encoded boolean histogram like "3;2;5;1,2;0:0,1:5,2:0"
    FORMAT(
      '{"bucket_count":%d,"histogram_type":%d,"sum":%d,"range":[%s],"values":{%s}}',
      CAST(SPLIT(input, ';')[SAFE_OFFSET(0)] AS INT64),
      CAST(SPLIT(input, ';')[SAFE_OFFSET(1)] AS INT64),
      CAST(SPLIT(input, ';')[SAFE_OFFSET(2)] AS INT64),
      CAST(SPLIT(input, ';')[SAFE_OFFSET(3)] AS STRING),
      ARRAY_TO_STRING(
        ARRAY(
          SELECT
            FORMAT(
              '"%d":%d',
              CAST(SPLIT(entry, ':')[SAFE_OFFSET(0)] AS INT64),
              CAST(SPLIT(entry, ':')[SAFE_OFFSET(1)] AS INT64)
            )
          FROM
            UNNEST(SPLIT(SPLIT(input, ';')[SAFE_OFFSET(4)], ',')) AS entry
          WHERE
            LENGTH(entry) >= 3
        ),
        ","
      )
    )
  WHEN
    ARRAY_LENGTH(SPLIT(input, ',')) = 2
  THEN
    -- Input is a compactly encoded boolean histogram like "0,5"
    FORMAT(
      '{"bucket_count":3,"histogram_type":2,"sum":%d,"range":[1,2],"values":{"0":%d,"1":%d,"2":0}}',
      CAST(SPLIT(input, ',')[SAFE_OFFSET(1)] AS INT64),
      CAST(SPLIT(input, ',')[SAFE_OFFSET(0)] AS INT64),
      CAST(SPLIT(input, ',')[SAFE_OFFSET(1)] AS INT64)
    )
  WHEN
    ARRAY_LENGTH(SPLIT(input, ',')) = 1
  THEN
    -- Input is a compactly encoded count histogram like "5"
    FORMAT(
      '{"bucket_count":3,"histogram_type":4,"sum":%d,"range":[1,2],"values":{"0":%d,"1":0}}',
      CAST(SPLIT(input, ',')[SAFE_OFFSET(0)] AS INT64),
      CAST(input AS INT64)
    )
  END
);

-- Tests
WITH test_data AS (
  SELECT
    '{"bucket_count":10,"histogram_type":1,"sum":2628,"range":[1,100],"values":{"0":12434,"1":297,"13":8}}' AS input
)
SELECT
  assert.equals(hist.string_to_json(input), input)
FROM
  test_data;

--
WITH test_data AS (
  SELECT
    "0,31" AS input,
    '{"bucket_count":3,"histogram_type":2,"sum":31,"range":[1,2],"values":{"0":0,"1":31,"2":0}}' AS expected
)
SELECT
  assert.equals(hist.string_to_json(input), expected)
FROM
  test_data;

--
WITH test_data AS (
  SELECT
    '51;1;0;1,50;' AS input,
    '{"bucket_count":51,"histogram_type":1,"sum":0,"range":[1,50],"values":{}}' AS expected
)
SELECT
  assert.equals(hist.string_to_json(input), expected)
FROM
  test_data;

--
WITH test_data AS (
  SELECT
    '3;2;5;1,2;0:0,1:5,2:0' AS input,
    '{"bucket_count":3,"histogram_type":2,"sum":5,"range":[1,2],"values":{"0":0,"1":5,"2":0}}' AS expected
)
SELECT
  assert.equals(hist.string_to_json(input), expected)
FROM
  test_data;

--
WITH test_data AS (
  SELECT
    '3' AS input,
    '{"bucket_count":3,"histogram_type":4,"sum":3,"range":[1,2],"values":{"0":3,"1":0}}' AS expected
)
SELECT
  assert.equals(hist.string_to_json(input), expected)
FROM
  test_data;
