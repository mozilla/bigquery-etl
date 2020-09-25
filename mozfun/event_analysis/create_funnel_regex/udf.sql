CREATE OR REPLACE FUNCTION event_analysis.create_funnel_regex(step_regexes ARRAY<STRING>, intermediate_steps BOOLEAN)
RETURNS STRING AS (
  IF(
    ARRAY_LENGTH(step_regexes) > 0,
    CONCAT('(', ARRAY_TO_STRING(step_regexes, IF(intermediate_steps, '(?:.*?)', '')), ')'),
    ''
  )
);

SELECT
  assert_equals('((?:a|b)(?:.*?)(?:b))', event_analysis.create_funnel_regex(['(?:a|b)', '(?:b)'], TRUE)),
  assert_equals('((?:a|b)(?:b))', event_analysis.create_funnel_regex(['(?:a|b)', '(?:b)'], FALSE)),
  assert_equals('', event_analysis.create_funnel_regex(CAST(NULL AS ARRAY<STRING>), FALSE)),
  assert_equals('', event_analysis.create_funnel_regex(CAST([] AS ARRAY<STRING>), FALSE)),
