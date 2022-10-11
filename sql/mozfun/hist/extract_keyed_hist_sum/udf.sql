CREATE OR REPLACE FUNCTION hist.extract_keyed_hist_sum(
  keyed_histogram ARRAY<STRUCT<key STRING, value STRING>>
)
RETURNS INT64 AS (
  (
    SELECT
      SUM(hist.extract_histogram_sum(value))
    FROM
      UNNEST(keyed_histogram)
  )
);

SELECT
  assert.true(
     hist.extract_keyed_hist_sum(
      [
        STRUCT("a" AS key,
               '{"bucket_count":3,"histogram_type":4,"sum":7,"range":[1,2],"values":{"0":0,"1":7}}' AS value),
        STRUCT("b" AS key,
               '{"bucket_count":3,"histogram_type":4,"sum":5,"range":[1,2],"values":{"0":0,"1":5}}' AS value)
      ]) = 12)
