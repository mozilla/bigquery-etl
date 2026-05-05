CREATE OR REPLACE FUNCTION assert.struct_equals(expected ANY TYPE, actual ANY TYPE) AS (
  IF(
    ARRAY_LENGTH(
      ARRAY(
        SELECT DISTINCT AS STRUCT
          *
        FROM
          (SELECT * FROM UNNEST([expected]) UNION ALL SELECT * FROM UNNEST([actual]))
      )
    ) = 1,
    TRUE,
    ERROR(CONCAT('Expected struct', TO_JSON_STRING(expected), ' but got ', TO_JSON_STRING(actual)))
  )
);

SELECT
  assert.struct_equals(STRUCT('a' AS a, 'b' AS b, 3 AS c), STRUCT('a' AS a, 'b' AS b, 3 AS c)),
  assert.struct_equals(STRUCT('a' AS a, NULL AS b, 3 AS c), STRUCT('a' AS a, NULL AS b, 3 AS c));

#xfail
SELECT
  assert.struct_equals(STRUCT('a' AS a, 'b' AS b, 3 AS c), STRUCT('a' AS a, 'b' AS b, 4 AS c))
