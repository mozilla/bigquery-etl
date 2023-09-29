CREATE OR REPLACE FUNCTION assert.array_equals(expected ANY TYPE, actual ANY TYPE) AS (
  IF(
    EXISTS(
      (
        SELECT
          *
        FROM
          UNNEST(expected)
          WITH OFFSET
        EXCEPT DISTINCT
        SELECT
          *
        FROM
          UNNEST(actual)
          WITH OFFSET
      )
      UNION ALL
        (
          SELECT
            *
          FROM
            UNNEST(actual)
            WITH OFFSET
          EXCEPT DISTINCT
          SELECT
            *
          FROM
            UNNEST(expected)
            WITH OFFSET
        )
    ),
    ERROR(CONCAT('Expected ', TO_JSON_STRING(expected), ' but got ', TO_JSON_STRING(actual))),
    TRUE
  )
);
