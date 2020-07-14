-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION
  jackknife.ratio_ci(
    n_buckets INT64,
    values_per_bucket ARRAY<STRUCT<numerator FLOAT64, denominator FLOAT64>>) AS (
      mozfun.jackknife.ratio_ci(n_bucket, values_per_bucket)
);