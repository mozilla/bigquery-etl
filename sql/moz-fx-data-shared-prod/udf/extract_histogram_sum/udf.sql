/*

This is a performance optimization compared to the more general
mozfun.hist.extract for cases where only the histogram sum is needed.

It must support all the same format variants as mozfun.hist.extract
but this simplification is necessary to keep the main_summary query complexity
in check.

*/
CREATE OR REPLACE FUNCTION udf.extract_histogram_sum(input STRING)
RETURNS INT64 AS (
  SAFE_CAST(
    COALESCE(
      JSON_EXTRACT_SCALAR(input, '$.sum'),
      SPLIT(input, ';')[SAFE_OFFSET(2)],
      SPLIT(input, ',')[SAFE_OFFSET(1)],
      input
    ) AS INT64
  )
);

-- Tests
WITH histogram AS (
  SELECT AS VALUE
    [
      '{"bucket_count":3,"histogram_type":4,"sum":7,"range":[1,2],"values":{"0":7,"1":0}}',
      '{"range":[1,2],"bucket_count":3,"histogram_type":4,"values":{"1": 7},"sum":7}',
      '4,7',
      '7',
      '3;2;7;1,2;0:0,1:5,2:1'
    ]
),
--
extracted AS (
  SELECT
    udf.extract_histogram_sum(h) AS hsum
  FROM
    histogram
  CROSS JOIN
    UNNEST(histogram) AS h
)
--
SELECT
  mozfun.assert.equals(7, hsum)
FROM
  extracted;

--
SELECT
  mozfun.assert.null(udf.extract_histogram_sum('foo'));
