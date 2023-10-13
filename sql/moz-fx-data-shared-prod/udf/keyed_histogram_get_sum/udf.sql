/*

Take a keyed histogram of type STRUCT<key STRING, value STRING>,
extract the histogram of the given key, and return the sum value

 */
CREATE OR REPLACE FUNCTION udf.keyed_histogram_get_sum(
  keyed_histogram ANY TYPE,
  target_key STRING
) AS (
  udf.extract_histogram_sum(udf.get_key(keyed_histogram, target_key))
);

-- Test
WITH histograms AS (
  SELECT
    [
      STRUCT(
        'key1' AS key,
        '{"bucket_count":3,"histogram_type":4,"sum":1,"range":[1,2],"values":{"0":1,"1":0}}' AS value
      ),
      STRUCT(
        'key2' AS key,
        '{"bucket_count":3,"histogram_type":4,"sum":2,"range":[1,2],"values":{"0":1,"1":1}}' AS value
      ),
      STRUCT('key3' AS key, '{}' AS value)
    ]
)
SELECT
  mozfun.assert.null(udf.keyed_histogram_get_sum(ARRAY<STRUCT<key STRING, value STRING>>[], 'dne')),
  mozfun.assert.null(udf.keyed_histogram_get_sum((SELECT * FROM histograms), 'dne')),
  mozfun.assert.equals(1, udf.keyed_histogram_get_sum((SELECT * FROM histograms), 'key1')),
  mozfun.assert.equals(2, udf.keyed_histogram_get_sum((SELECT * FROM histograms), 'key2')),
  mozfun.assert.null(udf.keyed_histogram_get_sum((SELECT * FROM histograms), 'key3'))
