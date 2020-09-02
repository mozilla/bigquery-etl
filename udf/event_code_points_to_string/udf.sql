CREATE OR REPLACE FUNCTION udf.event_code_points_to_string(code_points ANY TYPE) AS (
  CODE_POINTS_TO_STRING(
    ARRAY(
      SELECT
        CASE
          -- Skip commas
          WHEN i >= 43 THEN i + 2
          -- Skip double quote
          WHEN i >= 34 THEN i + 1
          ELSE i
        END
      FROM
        UNNEST(code_points) AS i
    )
  )
);

SELECT
  assert_equals("!", udf.event_code_points_to_string([33])),
  assert_equals("#", udf.event_code_points_to_string([34])),
  assert_equals("-", udf.event_code_points_to_string([43]))
