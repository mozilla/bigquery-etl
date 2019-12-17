/*

Return normalized engine name for recognized engines

*/

CREATE TEMP FUNCTION
  udf_normalize_search_engine(engine STRING) AS (
    IF(
        udf_strict_normalize_search_engine(engine) = 'Other',
        engine,
        udf_strict_normalize_search_engine(engine)
    )
  );

-- Test

SELECT
  assert_equals('Google', udf_normalize_search_engine('google')),
  assert_equals('Google', udf_normalize_search_engine('Google-abc')),
  assert_equals('not-bing', udf_normalize_search_engine('not-bing')),
  assert_equals('engine', udf_normalize_search_engine('engine')),
  assert_null(udf_normalize_search_engine(NULL))

