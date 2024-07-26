/*

Convert geoip lookup fields to a struct, replacing NULL with '??'.


Replaces NULL with '??' because '??' is a placeholder that may be used if there
was an issue during geoip lookup in hindsight.

*/
CREATE OR REPLACE FUNCTION udf.geo_struct_set_defaults(
  country STRING,
  city STRING,
  geo_subdivision1 STRING,
  geo_subdivision2 STRING
) AS ( --
  STRUCT(
    IFNULL(country, '??') AS country,
    IFNULL(city, '??') AS city,
    IFNULL(geo_subdivision1, '??') AS geo_subdivision1,
    IFNULL(geo_subdivision2, '??') AS geo_subdivision2
  )
);

-- Tests
SELECT
  mozfun.assert.equals(
    STRUCT('a' AS country, 'b' AS city, 'c' AS geo_subdivision1, 'd' AS geo_subdivision2),
    udf.geo_struct_set_defaults('a', 'b', 'c', 'd')
  ),
  mozfun.assert.equals(udf.geo_struct_set_defaults('??', 'b', 'c', 'd').country, '??'),
  mozfun.assert.equals(udf.geo_struct_set_defaults(NULL, 'b', 'c', 'd').country, '??'),
  mozfun.assert.equals(udf.geo_struct_set_defaults('a', 'b', 'c', 'd').country, 'a'),
  mozfun.assert.equals(udf.geo_struct_set_defaults('a', '??', 'c', 'd').city, '??'),
  mozfun.assert.equals(udf.geo_struct_set_defaults('a', NULL, 'c', 'd').city, '??'),
  mozfun.assert.equals(udf.geo_struct_set_defaults('a', 'b', 'c', 'd').city, 'b'),
  mozfun.assert.equals(udf.geo_struct_set_defaults('a', 'b', '??', 'd').geo_subdivision1, '??'),
  mozfun.assert.equals(udf.geo_struct_set_defaults('a', 'b', NULL, 'd').geo_subdivision1, '??'),
  mozfun.assert.equals(udf.geo_struct_set_defaults('a', 'b', 'c', 'd').geo_subdivision1, 'c'),
  mozfun.assert.equals(udf.geo_struct_set_defaults('a', 'b', 'c', '??').geo_subdivision2, '??'),
  mozfun.assert.equals(udf.geo_struct_set_defaults('a', 'b', 'c', NULL).geo_subdivision2, '??'),
  mozfun.assert.equals(udf.geo_struct_set_defaults('a', 'b', 'c', 'd').geo_subdivision2, 'd')
