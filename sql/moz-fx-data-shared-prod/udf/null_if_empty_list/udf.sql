/*

Return NULL if list is empty, otherwise return list.

This cannot be done with NULLIF because NULLIF does not support arrays.

*/
CREATE OR REPLACE FUNCTION udf.null_if_empty_list(list ANY TYPE) AS (
  IF(ARRAY_LENGTH(list) > 0, list, NULL)
);

-- Tests
SELECT
  mozfun.assert.null(udf.null_if_empty_list([]));
