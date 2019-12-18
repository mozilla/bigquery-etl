/*

Take a keyed histogram of type STRUCT<key STRING, value STRING>,
extract the histogram of the given key, and return the sum value

 */
CREATE TEMP FUNCTION udf_keyed_histogram_get_sum(keyed_histogram ANY TYPE, target_key STRING) AS (
  udf_json_extract_histogram(udf_get_key(keyed_histogram, target_key)).sum
);
-- Test
WITH histograms AS (
  SELECT [
    STRUCT('key1' AS key, '{"bucket_count":3,"histogram_type":4,"sum":1,"range":[1,2],"values":{"0":1,"1":0}}' AS value),
    STRUCT('key2' AS key, '{"bucket_count":3,"histogram_type":4,"sum":2,"range":[1,2],"values":{"0":1,"1":1}}' AS value),
    STRUCT('key3' AS key, '{}' AS value)
  ]
)

SELECT
  assert_null(udf_keyed_histogram_get_sum(ARRAY<STRUCT<key STRING, value STRING>>[], 'dne')),
  assert_null(udf_keyed_histogram_get_sum((SELECT * FROM histograms), 'dne')),
  assert_equals(
    1,
    udf_keyed_histogram_get_sum((SELECT * FROM histograms), 'key1')
  ),
  assert_equals(
    2,
    udf_keyed_histogram_get_sum((SELECT * FROM histograms), 'key2')
  ),
  assert_null(udf_keyed_histogram_get_sum((SELECT * FROM histograms), 'key3'))
