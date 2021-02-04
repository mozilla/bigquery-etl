/*
This struct represents the past year's worth of searches.
Each month has its own entry, hence 12.
*/
CREATE OR REPLACE FUNCTION udf.new_monthly_engine_searches_struct() AS (
  STRUCT(
    udf.array_of_12_zeroes() AS total_searches,
    udf.array_of_12_zeroes() AS tagged_searches,
    udf.array_of_12_zeroes() AS search_with_ads,
    udf.array_of_12_zeroes() AS ad_click
  )
);
