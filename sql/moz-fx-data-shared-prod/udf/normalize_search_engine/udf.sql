/*
Return normalized engine name for recognized engines

This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.normalize_search_engine(engine STRING) AS (
  CASE
    WHEN engine IS NULL
      THEN NULL
    WHEN STARTS_WITH(LOWER(engine), 'engine1')
      THEN 'Engine1'
    WHEN STARTS_WITH(LOWER(engine), 'engine2')
      THEN 'Engine2'
    WHEN STARTS_WITH(LOWER(engine), 'engine3')
      THEN 'Engine3'
    ELSE 'Other'
  END
);

-- Test
SELECT
  assert.equals('Engine1', udf.normalize_search_engine('engine1')),
  assert.equals('Engine2', udf.normalize_search_engine('Engine2-abc')),
  assert.equals('Other', udf.normalize_search_engine('not-Engine1')),
  assert.equals('Other', udf.normalize_search_engine('engine')),
  assert.null(udf.normalize_search_engine(NULL))
