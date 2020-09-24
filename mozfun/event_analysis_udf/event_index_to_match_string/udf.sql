CREATE OR REPLACE FUNCTION event_analysis_udf.event_index_to_match_string(index STRING)
RETURNS STRING AS (
  CONCAT(event_analysis_udf.escape_metachars(index), ',')
);

SELECT
  assert_equals('\\Qp\\E,', event_analysis_udf.event_index_to_match_string('p')),
  assert_equals('\\Q.\\E,', event_analysis_udf.event_index_to_match_string('.')),
  assert_equals('\\Q.t\\E,', event_analysis_udf.event_index_to_match_string('.t')),
  assert_equals(CAST(NULL AS STRING), event_analysis_udf.event_index_to_match_string(NULL)),
  assert_equals('\\Q\\E,', event_analysis_udf.event_index_to_match_string('')),
