/*

Return normalized engine name for recognized engines

*/

CREATE OR REPLACE FUNCTION
  udf.normalize_search_engine(engine STRING) AS (
    CASE
      WHEN engine IS NULL THEN NULL
      WHEN STARTS_WITH(LOWER(engine), 'google') THEN 'Google'
      WHEN STARTS_WITH(LOWER(engine), 'ddg') 
      OR STARTS_WITH(LOWER(engine), 'duckduckgo') THEN 'DuckDuckGo'
      WHEN STARTS_WITH(LOWER(engine), 'bing') THEN 'Bing'
      WHEN STARTS_WITH(LOWER(engine), 'yandex')
      OR STARTS_WITH(LOWER(engine), 'yasearch') THEN 'Yandex'
      WHEN STARTS_WITH(LOWER(engine), 'amazon') THEN 'Amazon'
      WHEN STARTS_WITH(LOWER(engine), 'ebay') THEN 'Ebay'
      ELSE 'Other'
    END
  );

-- Test

SELECT
  assert_equals('Google', udf.normalize_search_engine('google')),
  assert_equals('Google', udf.normalize_search_engine('Google-abc')),
  assert_equals('Other', udf.normalize_search_engine('not-bing')),
  assert_equals('Other', udf.normalize_search_engine('other-Google')),
  assert_equals('Other', udf.normalize_search_engine('engine')),
  assert_null(udf.normalize_search_engine(NULL))
