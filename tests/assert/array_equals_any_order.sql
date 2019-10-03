CREATE TEMP FUNCTION
  assert_array_equals_any_order(expected ANY TYPE, actual ANY TYPE) AS (
    assert_array_equals(
      (SELECT ARRAY_AGG(e ORDER BY e)
      FROM UNNEST(expected) AS e)
    , (SELECT ARRAY_AGG(e ORDER BY e)
      FROM UNNEST(actual) AS e))
);
