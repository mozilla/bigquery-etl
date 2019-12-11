/*
This struct represents the past year's worth of searches.
Each month has its own entry, hence 12.
*/

CREATE TEMP FUNCTION  udf_new_monthly_engine_searches_struct() AS (
  STRUCT(
    udf_12_zeroes() AS total_searches,
    udf_12_zeroes() AS tagged_searches,
    udf_12_zeroes() AS search_with_ads,
    udf_12_zeroes() AS ad_click
  )
);
