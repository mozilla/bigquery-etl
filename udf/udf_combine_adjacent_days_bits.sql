CREATE TEMP FUNCTION
  udf_combine_adjacent_days_bits(prev INT64,
    curr INT64) AS (udf_shift_bits_one_day(prev) + IFNULL(curr,
    0));
