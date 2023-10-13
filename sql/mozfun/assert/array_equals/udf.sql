CREATE OR REPLACE FUNCTION assert.array_equals(expected ANY TYPE, actual ANY TYPE) AS (
  IF(
    EXISTS(
      (
        SELECT
          *
        FROM
          UNNEST(expected)
          WITH OFFSET AS offset
        EXCEPT DISTINCT
        SELECT
          *
        FROM
          UNNEST(actual)
          WITH OFFSET AS offset
      )
      UNION ALL
        (
          SELECT
            *
          FROM
            UNNEST(actual)
            WITH OFFSET AS offset
          EXCEPT DISTINCT
          SELECT
            *
          FROM
            UNNEST(expected)
            WITH OFFSET AS offset
        )
    ),
    ERROR(CONCAT('Expected ', TO_JSON_STRING(expected), ' but got ', TO_JSON_STRING(actual))),
    TRUE
  )
);
