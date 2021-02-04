/*

Return the number of recorded observations greater than threshold for the
histogram.

CAUTION: Does not count any buckets that have any values less than the
threshold. For example, a bucket with range (1, 10) will not be counted for a
threshold of 2. Use threshold that are not bucket boundaries with caution.

https://github.com/mozilla/telemetry-batch-view/blob/ea0733c/src/main/scala/com/mozilla/telemetry/utils/MainPing.scala#L213-L239

*/
CREATE OR REPLACE FUNCTION hist.threshold_count(histogram STRING, threshold INT64) AS (
  (SELECT IFNULL(SUM(value), 0) FROM UNNEST(hist.extract(histogram).values) WHERE key >= threshold)
);

-- Tests
SELECT
  assert.equals(17, hist.threshold_count('{"values":{"0":1,"1":2, "4": 10, "8": 7}}', 4)),
  assert.equals(0, hist.threshold_count('{}', 1)),
  assert.equals(0, hist.threshold_count('{"values":{"0":0}}', 1)),
  assert.equals(3, hist.threshold_count('{"values":{"5":1, "6":3}}', 6))
