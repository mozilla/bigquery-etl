/*

Returns a parsed struct from a JSON string representing a histogram.

This implementation uses JavaScript and is provided for performance comparison;
see udf/udf_json_extract_histogram for a pure SQL implementation that will likely
be more usable in practice.

*/
CREATE OR REPLACE FUNCTION udf_js.json_extract_histogram(input STRING)
RETURNS STRUCT<
  bucket_count INT64,
  histogram_type INT64,
  `sum` INT64,
  `range` ARRAY<INT64>,
  `values` ARRAY<STRUCT<key INT64, value INT64>>
> DETERMINISTIC
LANGUAGE js
AS
  """
    if (input == null) {
      return null;
    }
    var result = JSON.parse(input);
    var valuesMap = result.values;
    var valuesArray = [];
    for (var key in valuesMap) {
      valuesArray.push({"key": parseInt(key), "value": valuesMap[key]})
    }
    result.values = valuesArray;
    return result;
""";

-- Tests
WITH histogram AS (
  SELECT AS VALUE
    '{"bucket_count":10,"histogram_type":1,"sum":2628,"range":[1,100],"values":{"0":12434,"1":297,"13":8}}'
),
  --
extracted AS (
  SELECT
    udf_js.json_extract_histogram(histogram).*
  FROM
    histogram
)
  --
SELECT
  mozfun.assert.equals(10, bucket_count),
  mozfun.assert.equals(1, histogram_type),
  mozfun.assert.equals(2628, `sum`),
  mozfun.assert.array_equals([1, 100], `range`),
  mozfun.assert.array_equals(
    [
      STRUCT(0 AS key, 12434 AS value),
      STRUCT(1 AS key, 297 AS value),
      STRUCT(13 AS key, 8 AS value)
    ],
    `values`
  )
FROM
  extracted
