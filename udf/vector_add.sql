CREATE TEMP FUNCTION
  udf_vector_add(a ARRAY<INT64>, b ARRAY<INT64>) AS ((
    with a_unnested AS (
      SELECT _a, _a_off
      FROM UNNEST(a) AS _a WITH OFFSET _a_off
    ), b_unnested AS (
      SELECT _b, _b_off
      FROM UNNEST(b) AS _b WITH OFFSET _b_off
    )

    SELECT ARRAY_AGG(COALESCE(_a + _b, _a, _b) ORDER BY COALESCE(_a_off, _b_off) ASC)
    FROM a_unnested
    FULL OUTER JOIN b_unnested
      ON _a_off = _b_off
    ));

-- 

SELECT
  assert_array_equals(ARRAY [2, 3, 4], udf_vector_add(ARRAY [1, 2, 3], ARRAY [1, 1, 1])),
  assert_array_equals(ARRAY [2, 3, 4, 1], udf_vector_add(ARRAY [1, 2, 3], ARRAY [1, 1, 1, 1])),
  assert_array_equals(ARRAY [2, 3, 4, 4], udf_vector_add(ARRAY [1, 2, 3, 4], ARRAY [1, 1, 1])),
  assert_array_equals(ARRAY [2, 3, 1], udf_vector_add(ARRAY [1, 2, NULL], ARRAY [1, 1, 1])),
  assert_array_equals(ARRAY [1, 1, 1], udf_vector_add(NULL, ARRAY [1, 1, 1])),
  assert_array_equals(ARRAY [1, 1, NULL], udf_vector_add(NULL, ARRAY [1, 1, NULL])),
  assert_array_equals(ARRAY [2, 3, NULL, 4], udf_vector_add(ARRAY [1, 2, NULL, 3], ARRAY [1, 1, NULL, 1]));
