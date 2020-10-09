CREATE OR REPLACE FUNCTION event_analysis.escape_metachars(s STRING)
RETURNS STRING AS (
  CONCAT('\\Q', s, '\\E')
);

-- Tests
SELECT
  assert.equals('\\Q.*\\E', event_analysis.escape_metachars('.*')),
  assert.equals(CAST(NULL AS STRING), event_analysis.escape_metachars(NULL)),
  assert.equals('\\Q\\E', event_analysis.escape_metachars('')),
  assert.equals('$', regexp_extract('.$*', event_analysis.escape_metachars('$'))),
  assert.equals(
    CAST(NULL AS STRING),
    regexp_extract('.$*', event_analysis.escape_metachars('...'))
  ),
  assert.equals(
    '.$*',
    regexp_extract(
      '.$*',
      CONCAT(event_analysis.escape_metachars('.'), '.', event_analysis.escape_metachars('*'))
    )
  ),
