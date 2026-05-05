/*

Returns an array of parsed structs from a JSON string representing a keyed histogram.

This is likely only useful for histograms that weren't properly parsed to fields,
so ended up embedded in an additional_properties JSON blob. Normally, keyed histograms
will be modeled as a key/value struct where the values are JSON representations of
single histograms. There is no pure SQL equivalent to this function, since BigQuery
does not provide any functions for listing or iterating over keys in a JSON map.

*/
CREATE OR REPLACE FUNCTION udf_js.json_extract_keyed_histogram(input STRING)
RETURNS ARRAY<
  STRUCT<
    key STRING,
    bucket_count INT64,
    histogram_type INT64,
    `sum` INT64,
    `range` ARRAY<INT64>,
    `values` ARRAY<STRUCT<key INT64, value INT64>>
  >
> DETERMINISTIC
LANGUAGE js
AS
  """
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
    return result;
""";

-- Tests
WITH keyed_histogram AS (
  SELECT AS VALUE
    '{"audio/mp4a-latm":{"bucket_count":3,"histogram_type":4,"sum":3,"range":[1,2],"values":{"0":3,"1":0}}}'
),
  --
extracted AS (
  SELECT
    histogram.*
  FROM
    keyed_histogram,
    UNNEST(udf_js.json_extract_keyed_histogram(keyed_histogram)) AS histogram
)
  --
SELECT
  mozfun.assert.equals('audio/mp4a-latm', `key`),
  mozfun.assert.equals(3, bucket_count),
  mozfun.assert.equals(4, histogram_type),
  mozfun.assert.equals(3, `sum`),
  mozfun.assert.array_equals([1, 2], `range`),
  mozfun.assert.array_equals([STRUCT(0 AS key, 3 AS value), STRUCT(1 AS key, 0 AS value)], `values`)
FROM
  extracted
