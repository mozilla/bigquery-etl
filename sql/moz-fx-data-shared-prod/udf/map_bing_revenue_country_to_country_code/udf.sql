/*
For use by LTV revenue join only.

Maps the Bing country to a country code.
Only keeps the country codes we want to aggregate on.
*/
CREATE OR REPLACE FUNCTION udf.map_bing_revenue_country_to_country_code(country STRING) AS (
  CASE
    country
    WHEN 'CTY_United States'
      THEN 'US'
    WHEN 'CTY_Germany'
      THEN 'DE'
    WHEN 'CTY_United_Kingdom'
      THEN 'UK'
    WHEN 'CTY_France'
      THEN 'FR'
    WHEN 'CTY_Canada'
      THEN 'CA'
    ELSE 'Other'
  END
);

-- Tests
SELECT
  mozfun.assert.equals('US', udf.map_bing_revenue_country_to_country_code('CTY_United States')),
  mozfun.assert.equals('Other', udf.map_bing_revenue_country_to_country_code('CTY_Bolivia'))
