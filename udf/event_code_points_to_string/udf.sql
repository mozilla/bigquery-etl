CREATE OR REPLACE FUNCTION udf.event_code_points_to_string(code_points ANY TYPE) AS (
  CODE_POINTS_TO_STRING(
    IF(
      code_points IS NULL,
      NULL,
      ARRAY(
        SELECT
          CASE
          WHEN
            i IS NULL
          THEN
            NULL
            -- Skip commas
          WHEN
            i >= 43
          THEN
            i + 2
            -- Skip double quote
          WHEN
            i >= 34
          THEN
            i + 1
          ELSE
            i
          END
        FROM
          UNNEST(code_points) AS i
      )
    )
  )
);

SELECT
  assert_equals("!", udf.event_code_points_to_string([33])),
  assert_equals("#", udf.event_code_points_to_string([34])),
  assert_equals("-", udf.event_code_points_to_string([43])),
  assert_equals(CODE_POINTS_TO_STRING([1, 2]), udf.event_code_points_to_string([1, 2])),
  assert_equals(
    CODE_POINTS_TO_STRING(NULL),
    udf.event_code_points_to_string(CAST(NULL AS ARRAY<INT64>))
  ),
  assert_equals(CODE_POINTS_TO_STRING([NULL]), udf.event_code_points_to_string([NULL])),
  assert_equals(CODE_POINTS_TO_STRING([]), udf.event_code_points_to_string([])),
