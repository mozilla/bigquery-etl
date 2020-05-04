/*
This assumes a convention where a nested array is actually a STRUCT with an `arr` field.
That field contains the nested array.

This is a workaround becuase BigQuery does not support nested arrays.
*/
CREATE TEMP FUNCTION assert_nested_array_equals(expected ANY TYPE, actual ANY TYPE) AS (
  IF(
    EXISTS(
      SELECT
        assert_array_equals(e.arr, a.arr)
      FROM
        UNNEST(expected) AS e
        WITH OFFSET e_offset,
        UNNEST(actual) AS a
        WITH OFFSET a_offset
      WHERE
        e_offset = a_offset
    ),
    TRUE,
    ERROR(CONCAT('Expected ', TO_JSON_STRING(expected), ' but got ', TO_JSON_STRING(actual)))
  )
);
