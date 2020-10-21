-- udf_linear_buckets
CREATE OR REPLACE FUNCTION glam.histogram_generate_linear_buckets(
  min FLOAT64,
  max FLOAT64,
  nBuckets FLOAT64
)
RETURNS ARRAY<FLOAT64> DETERMINISTIC
LANGUAGE js
AS
  '''
  let result = [0];
  for (let i = 1; i < Math.min(nBuckets, max, 10000); i++) {
    let linearRange = (min * (nBuckets - 1 - i) + max * (i - 1)) / (nBuckets - 2);
    result.push(Math.round(linearRange));
  }
  return result;
''';
