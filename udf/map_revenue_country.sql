CREATE OR REPLACE FUNCTION udf.map_revenue_country(engine STRING, country STRING) AS (
  CASE
    engine
  WHEN
    'Google'
  THEN
    IF(country = 'US', 'US', 'ROW')
  WHEN
    'Bing'
  THEN
    IF(country IN ('US', 'DE', 'UK', 'FR', 'CA'), country, 'Other')
  WHEN
    'Other'
  THEN
    country
  WHEN
    'DuckDuckGo'
  THEN
    country
  WHEN
    'Yandex'
  THEN
    country
  ELSE
    ERROR(CONCAT("Engine ", COALESCE(engine, "null"), " is not aggregated at this time"))
  END
);

-- Tests
SELECT
  assert_equals('US', udf.map_revenue_country('Google', 'US')),
  assert_equals('US', udf.map_revenue_country('Bing', 'US')),
  assert_equals('Other', udf.map_revenue_country('Bing', 'AU')),
  assert_equals('ROW', udf.map_revenue_country('Google', 'AU'))
