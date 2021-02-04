CREATE OR REPLACE FUNCTION event_analysis.event_index_to_match_string(index STRING)
RETURNS STRING AS (
  CONCAT(event_analysis.escape_metachars(index), ',')
);

SELECT
  assert.equals('\\Qp\\E,', event_analysis.event_index_to_match_string('p')),
  assert.equals('\\Q.\\E,', event_analysis.event_index_to_match_string('.')),
  assert.equals('\\Q.t\\E,', event_analysis.event_index_to_match_string('.t')),
  assert.equals(CAST(NULL AS STRING), event_analysis.event_index_to_match_string(NULL)),
  assert.equals('\\Q\\E,', event_analysis.event_index_to_match_string('')),
