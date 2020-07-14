-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION jackknife.sum_ci(n_buckets INT64, counts_per_bucket ARRAY<INT64>) AS (
  mozfun.jackknife.sum_ci(n_buckets, counts_per_bucket)
);
