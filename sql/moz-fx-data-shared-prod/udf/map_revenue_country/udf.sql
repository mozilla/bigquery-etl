/*
Only for use by the LTV Revenue join.

Maps country codes to the codes we have in the revenue dataset.
Buckets small Bing countries into "other".
*/
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
    NULL
  END
);

-- Tests
SELECT
  assert.equals('US', udf.map_revenue_country('Google', 'US')),
  assert.equals('US', udf.map_revenue_country('Bing', 'US')),
  assert.equals('Other', udf.map_revenue_country('Bing', 'AU')),
  assert.equals('ROW', udf.map_revenue_country('Google', 'AU')),
  assert.equals(CAST(NULL AS STRING), udf.map_revenue_country('Amazon', 'US'))
