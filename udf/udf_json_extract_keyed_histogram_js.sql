/*

Returns an array of parsed structs from a JSON string representing a keyed histogram.

*/

CREATE TEMP FUNCTION
  udf_json_extract_keyed_histogram_js (input STRING)
  RETURNS ARRAY<STRUCT<key STRING,
  bucket_count INT64,
  histogram_type INT64,
  `sum` INT64,
  `range` ARRAY<INT64>,
  `values` ARRAY<STRUCT<key INT64,
  value INT64>> > >
  LANGUAGE js AS """
    if (input == null) {
      return null;
    }
    var result = [];
    var parsed = JSON.parse(input);
    for (var histoKey in parsed) {
      var histogram = parsed[histoKey]
      var valuesMap = histogram.values;
      var valuesArray = [];
      for (var key in valuesMap) {
        valuesArray.push({"key": parseInt(key), "value": valuesMap[key]})
      }
      histogram.values = valuesArray;
      histogram.key = histoKey
      result.push(histogram)
    }
    result.values = valuesArray;
    return result;
""";

-- Tests

WITH
  keyed_histogram AS (
    SELECT AS VALUE
      '{"audio/mp4a-latm":{"bucket_count":3,"histogram_type":4,"sum":3,"range":[1,2],"values":{"0":3,"1":0}}}' ),
  --
  extracted AS (
    SELECT
      histogram.*
    FROM
      keyed_histogram,
      UNNEST(udf_json_extract_keyed_histogram_js(keyed_histogram)) AS histogram )
  --
SELECT
  assert_equals('audio/mp4a-latm', `key`),
  assert_equals(3, bucket_count),
  assert_equals(4, histogram_type),
  assert_equals(3, `sum`),
  assert_array_equals([1, 2], `range`)
FROM
  extracted
