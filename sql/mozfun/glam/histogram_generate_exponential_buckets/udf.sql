-- udf_exponential_buckets
CREATE OR REPLACE FUNCTION glam.histogram_generate_exponential_buckets(
  min FLOAT64,
  max FLOAT64,
  nBuckets FLOAT64
)
RETURNS ARRAY<FLOAT64> DETERMINISTIC
LANGUAGE js
AS
  '''
  let logMax = Math.log(max);
  let current = min;
  if (current === 0) {
    current = 1;
  } // If starting from 0, the second bucket should be 1 rather than 0
  let retArray = [0, current];
  for (let bucketIndex = 2; bucketIndex < Math.min(nBuckets, max, 10000); bucketIndex++) {
    let logCurrent = Math.log(current);
    let logRatio = (logMax - logCurrent) / (nBuckets - bucketIndex);
    let logNext = logCurrent + logRatio;
    let nextValue =  Math.round(Math.exp(logNext));
    current = nextValue > current ? nextValue : current + 1;
    retArray[bucketIndex] = current;
  }
  return retArray
''';

SELECT
-- Buckets of GC_MS
-- https://sql.telemetry.mozilla.org/queries/75802/source
  assert.array_equals(
    [
      --format:off
      0,1,2,3,4,5,6,7,8,10,12,14,17,20,24,29,34,40,48,57,68,81,96,114,135,160,
      190,226,268,318,378,449,533,633,752,894,1062,1262,1500,1782,2117,2516,
      2990,3553,4222,5017,5961,7083,8416,10000
      --format:on
    ],
    glam.histogram_generate_exponential_buckets(0, 10000, 50)
  ),
  -- smaller number of buckets
  assert.array_equals(
    [0, 1, 3, 10, 32, 101, 319, 1006, 3172, 10000],
    glam.histogram_generate_exponential_buckets(0, 10000, 10)
  ),
  -- starting at 1 still includes a 0 bucket
  assert.array_equals(
    [0, 1, 3, 10, 32, 101, 319, 1006, 3172, 10000],
    glam.histogram_generate_exponential_buckets(1, 10000, 10)
  )
