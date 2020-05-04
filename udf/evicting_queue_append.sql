/*
An evicting queue stores at most `size` elements, following FIFO order.

If queue_length < size, it appends the item.
If queue_length = size, it appends the item and drops the first element.
*/
CREATE OR REPLACE FUNCTION udf.evicting_queue_append(queue ANY TYPE, size INT64, item ANY TYPE) AS (
  ARRAY(
    SELECT
      val
    FROM
      UNNEST(queue | | [item]) AS val
      WITH OFFSET AS offset
    WHERE
      NOT (ARRAY_LENGTH(queue) = size AND offset = 0)
    ORDER BY
      offset ASC
  )
);

-- Test
SELECT
  assert_array_equals(['a', 'b', 'c'], udf.evicting_queue_append(['a', 'b'], 3, 'c')),
  assert_array_equals(['b', 'c'], udf.evicting_queue_append(['a', 'b'], 2, 'c')),
  assert_array_equals(['c'], udf.evicting_queue_append(CAST([] AS ARRAY<STRING>), 3, 'c')),
  assert_array_equals(
    CAST([] AS ARRAY<STRING>),
    udf.evicting_queue_append(CAST([] AS ARRAY<STRING>), 0, 'c')
  ),
  assert_array_equals([1, 2, 3], udf.evicting_queue_append([1, 2], 3, 3)),
  assert_nested_array_equals(
    [STRUCT([1, 2] AS arr), STRUCT([2, 3] AS arr)],
    udf.evicting_queue_append([STRUCT([1, 2] AS arr)], 3, STRUCT([2, 3] AS arr))
  ),
  assert_nested_array_equals(
    [STRUCT([2, 3] AS arr)],
    udf.evicting_queue_append([STRUCT([1, 2] AS arr)], 1, STRUCT([2, 3] AS arr))
  );
