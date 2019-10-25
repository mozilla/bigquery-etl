CREATE TEMP FUNCTION  udf_engine_searches_struct() AS (
  STRUCT(
    udf_zeroed_array(12) AS total_searches,
    udf_zeroed_array(12) AS tagged_searches,
    udf_zeroed_array(12) AS search_with_ads,
    udf_zeroed_array(12) AS ad_click
  )
);
