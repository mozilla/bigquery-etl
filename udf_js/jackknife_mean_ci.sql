-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf_js.jackknife_mean_ci(n_buckets INT64, values_per_bucket ARRAY<FLOAT64>) AS (
  mozfun.jackknife.mean_ci(n_buckets, values_per_bucket)
);
