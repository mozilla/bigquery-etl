CREATE OR REPLACE FUNCTION assert.array_equals_any_order(expected ANY TYPE, actual ANY TYPE) AS (
  assert.array_equals(
    (SELECT ARRAY_AGG(e ORDER BY e) FROM UNNEST(expected) AS e),
    (SELECT ARRAY_AGG(e ORDER BY e) FROM UNNEST(actual) AS e)
  )
);
