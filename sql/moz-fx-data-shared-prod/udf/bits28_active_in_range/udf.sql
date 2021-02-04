-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.bits28_active_in_range(
  bits INT64,
  start_offset INT64,
  n_bits INT64
) AS (
  mozfun.bits28.active_in_range(bits, start_offset, n_bits)
);
