CREATE OR REPLACE FUNCTION event_analysis.event_property_value_to_match_string(
  event_index STRING,
  property_index INTEGER,
  property_value STRING
)
RETURNS STRING AS (
  CONCAT(
    event_analysis.escape_metachars(property_value),
    -- Event property values are stored in `events_daily.events` strings in reverse order,
    -- so we expect the Nth property value to be followed by N-1 other property values.
    IF(property_index > 1, CONCAT('[^,]{', (property_index - 1), '}'), ''),
    event_analysis.event_index_to_match_string(event_index)
  )
);

SELECT
  assert.equals(r'\Qp\E\Qe\E,', event_analysis.event_property_value_to_match_string('e', 1, 'p')),
  assert.equals(
    r'\Qp\E[^,]{1}\Qe\E,',
    event_analysis.event_property_value_to_match_string('e', 2, 'p')
  ),
  assert.equals(
    CAST(NULL AS STRING),
    event_analysis.event_property_value_to_match_string('e', 1, NULL)
  ),
  assert.equals(
    'pe,',
    REGEXP_EXTRACT('""pe,', event_analysis.event_property_value_to_match_string('e', 1, 'p'))
  ),
  assert.equals(
    CAST(NULL AS STRING),
    REGEXP_EXTRACT('""pe,', event_analysis.event_property_value_to_match_string('e', 2, 'p'))
  ),
  assert.equals(
    CAST(NULL AS STRING),
    REGEXP_EXTRACT('"p"e,', event_analysis.event_property_value_to_match_string('e', 1, 'p'))
  ),
  assert.equals(
    'p"e,',
    REGEXP_EXTRACT('"p"e,', event_analysis.event_property_value_to_match_string('e', 2, 'p'))
  ),
