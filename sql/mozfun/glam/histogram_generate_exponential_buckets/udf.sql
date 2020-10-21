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
