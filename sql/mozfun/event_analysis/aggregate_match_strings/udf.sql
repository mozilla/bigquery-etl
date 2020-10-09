CREATE OR REPLACE FUNCTION event_analysis.aggregate_match_strings(match_strings ARRAY<STRING>)
RETURNS STRING AS (
  IF(ARRAY_LENGTH(match_strings) > 0, CONCAT('(?:', ARRAY_TO_STRING(match_strings, '|'), ')'), '')
);

-- Tests
SELECT
  assert.equals('(?:a|b)', event_analysis.aggregate_match_strings(['a', 'b'])),
  assert.equals('', event_analysis.aggregate_match_strings(CAST(NULL AS ARRAY<STRING>))),
  assert.equals('', event_analysis.aggregate_match_strings(CAST([] AS ARRAY<STRING>))),
