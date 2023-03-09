/*
Return normalized engine name for recognized engines

This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.normalize_search_engine(engine STRING) AS (
  CASE
    WHEN engine IS NULL
      THEN NULL
    ELSE 'Other'
  END
);

-- Test
SELECT
  assert.equals('Other', udf.normalize_search_engine('not-Engine1')),
  assert.equals('Other', udf.normalize_search_engine('engine')),
  assert.null(udf.normalize_search_engine(NULL))
