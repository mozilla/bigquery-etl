/*

Return normalized engine name for recognized engines

*/

CREATE TEMP FUNCTION
  udf_normalize_search_engine(engine STRING) AS (
    CASE
      WHEN STARTS_WITH(engine, 'google')
      OR STARTS_WITH(engine, 'Google')
      OR STARTS_WITH(engine, 'other-Google') THEN 'Google'
      WHEN STARTS_WITH(engine, 'ddg')
      OR STARTS_WITH(engine, 'duckduckgo')
      OR STARTS_WITH(engine, 'DuckDuckGo')
      OR STARTS_WITH(engine, 'other-DuckDuckGo') THEN 'DuckDuckGo'
      WHEN STARTS_WITH(engine, 'bing')
      OR STARTS_WITH(engine, 'Bing')
      OR STARTS_WITH(engine, 'other-Bing') THEN 'Bing'
      WHEN STARTS_WITH(engine, 'yandex')
      OR STARTS_WITH(engine, 'Yandex')
      OR STARTS_WITH(engine, 'other-Yandex') THEN 'Yandex'
      ELSE engine
    END
  );

-- Test

SELECT
  assert_equals('Google', udf_normalize_search_engine('google')),
  assert_equals('Google', udf_normalize_search_engine('Google-abc')),
  assert_equals('not-bing', udf_normalize_search_engine('not-bing')),
  assert_equals('engine', udf_normalize_search_engine('engine'))

