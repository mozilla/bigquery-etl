/*

Convert geoip lookup fields to a struct, replacing '??' with NULL.

Returns NULL if if required field country would be NULL.

Replaces '??' with NULL because '??' is a placeholder that may be used if there
was an issue during geoip lookup in hindsight.

*/
CREATE OR REPLACE FUNCTION udf.geo_struct(
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

-- Tests
SELECT
  mozfun.assert.equals(
    STRUCT('a' AS country, 'b' AS city, 'c' AS geo_subdivision1, 'd' AS geo_subdivision2),
    udf.geo_struct('a', 'b', 'c', 'd')
  ),
  mozfun.assert.null(udf.geo_struct('??', 'b', 'c', 'd')),
  mozfun.assert.null(udf.geo_struct('a', '??', 'c', 'd').city),
  mozfun.assert.null(udf.geo_struct('a', 'b', '??', 'd').geo_subdivision1),
  mozfun.assert.null(udf.geo_struct('a', 'b', 'c', '??').geo_subdivision2)
