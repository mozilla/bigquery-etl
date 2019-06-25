/*

Returns a parsed struct from a JSON string representing a histogram.

The built-in BigQuery JSON parsing functions are not powerful enough to handle
all the logic here, so we resort to some string processing. This function could
behave unexpectedly on poorly-formatted histogram JSON, but we expect that
payload validation in the data pipeline should ensure that histograms are well
formed, which gives us some flexibility here.

The only "correct" way to fully parse JSON strings in BigQuery is via JS UDFs;
we provide a JS implementation udf_js_json_extract_histogram for comparison,
but we expect that the overhead of the JS sandbox means that the pure SQL
implementation here will have better performance.

*/

CREATE TEMP FUNCTION
  udf_json_extract_histogram (input STRING) AS (STRUCT(
    CAST(JSON_EXTRACT_SCALAR(input, '$.bucket_count') AS INT64) AS bucket_count,
    CAST(JSON_EXTRACT_SCALAR(input, '$.histogram_type') AS INT64) AS histogram_type,
    CAST(JSON_EXTRACT_SCALAR(input, '$.sum') AS INT64) AS `sum`,
    ARRAY(
      SELECT
        CAST(bound AS INT64)
      FROM
        UNNEST(SPLIT(TRIM(JSON_EXTRACT(input, '$.range'), '[]'), ',')) AS bound) AS `range`,
    udf_json_extract_int_map(JSON_EXTRACT(input, '$.values')) AS `values` ));

-- Tests

WITH
  histogram AS (
    SELECT AS VALUE
      '{"bucket_count":10,"histogram_type":1,"sum":2628,"range":[1,100],"values":{"0":12434,"1":297,"13":8}}' ),
  --
  extracted AS (
     SELECT
       udf_json_extract_histogram(histogram).*
     FROM
       histogram )
  --
SELECT
  assert_equals(10, bucket_count),
  assert_equals(1, histogram_type),
  assert_equals(2628, `sum`),
  assert_array_equals([1, 100], `range`),
  assert_array_equals([STRUCT(0 AS key, 12434 AS value),
                       STRUCT(1 AS key, 297 AS value),
                       STRUCT(13 AS key, 8 AS value)],
                      `values`)
FROM
  extracted
