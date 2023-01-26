CREATE OR REPLACE FUNCTION event_analysis.escape_metachars(s STRING)
RETURNS STRING AS (
  CONCAT('\\Q', s, '\\E')
);

-- Tests
SELECT
  assert.equals('\\Q.*\\E', event_analysis.escape_metachars('.*')),
  assert.equals(CAST(NULL AS STRING), event_analysis.escape_metachars(NULL)),
  assert.equals('\\Q\\E', event_analysis.escape_metachars('')),
  assert.equals('$', REGEXP_EXTRACT('.$*', event_analysis.escape_metachars('$'))),
  assert.equals(
    CAST(NULL AS STRING),
    REGEXP_EXTRACT('.$*', event_analysis.escape_metachars('...'))
  ),
  assert.equals(
    '.$*',
    REGEXP_EXTRACT(
      '.$*',
      CONCAT(event_analysis.escape_metachars('.'), '.', event_analysis.escape_metachars('*'))
    )
  ),
