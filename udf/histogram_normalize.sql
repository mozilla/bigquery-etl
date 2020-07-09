-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.histogram_normalize(
  histogram STRUCT<
    bucket_count INT64,
    `sum` INT64,
    histogram_type INT64,
    `range` ARRAY<INT64>,
    `values` ARRAY<STRUCT<key INT64, value INT64>>
  >
)
RETURNS STRUCT<
  bucket_count INT64,
  `sum` INT64,
  histogram_type INT64,
  `range` ARRAY<INT64>,
  `values` ARRAY<STRUCT<key INT64, value FLOAT64>>
> AS (
  mozfun.hist.normalize()
);
