CREATE TEMP FUNCTION udf_geo_struct(
  country STRING,
  city STRING,
  geo_subdivision1 STRING,
  geo_subdivision2 STRING
) AS ( --
  IF(
    country IS NULL
    OR country = '??',
    NULL,
    STRUCT(
      country,
      NULLIF(city, '??') AS city,
      NULLIF(geo_subdivision1, '??') AS geo_subdivision1,
      NULLIF(geo_subdivision2, '??') AS geo_subdivision2
    )
  )
);

/*

Convert geoip lookup fields to a struct, replacing '??' with NULL.

Returns NULL if if required field country would be NULL.

Replaces '??' with NULL because '??' is a placeholder that may be used if there
was an issue during geoip lookup in hindsight.

*/
