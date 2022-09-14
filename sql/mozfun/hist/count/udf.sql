-- Extracts the values from the histogram and sums them, returning the total_count.
CREATE OR REPLACE FUNCTION hist.count(histogram STRING)
RETURNS INT64 AS (
  (
    WITH total_counts AS (
      SELECT
        sum(a.value) AS total_count
      FROM
        UNNEST(mozfun.hist.extract(histogram).values) AS a
    )
    SELECT
      total_count
    FROM
      total_counts
  )
);

-- Tests
WITH histogram AS (
  SELECT AS VALUE
    [
      '{"bucket_count":3,"histogram_type":4,"sum":7,"range":[1,2],"values":{"0":7,"1":0}}',
      '{"bucket_count":3,"histogram_type":4,"sum":7,"range":[1,2],"values":{"0":2,"1":5}}',
      '{"range":[1,2],"bucket_count":3,"histogram_type":4,"values":{"1": 7},"sum":7}',
      '4,3',
      '7',
      '3;2;7;1,2;0:1,1:2,2:4'
    ]
),
--
extracted AS (
  SELECT
    hist.count(h) AS hcount
  FROM
    histogram
  CROSS JOIN
    UNNEST(histogram) AS h
)
--
SELECT
  assert.equals(8, hcount)
FROM
  extracted;
