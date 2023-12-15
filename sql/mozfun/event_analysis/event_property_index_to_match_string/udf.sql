CREATE OR REPLACE FUNCTION event_analysis.event_property_index_to_match_string(
  event_index STRING,
  property_index INTEGER
)
RETURNS STRING AS (
  CONCAT(
    -- Double-quote characters are used in `events_daily.events` strings to indicate missing properties.
    '[^",]',
    -- Event property values are stored in `events_daily.events` strings in reverse order,
    -- so we expect the Nth property value to be followed by N-1 other property values.
    IF(property_index > 1, CONCAT('[^,]{', (property_index - 1), '}'), ''),
    event_analysis.event_index_to_match_string(event_index)
  )
);

SELECT
  assert.equals(r'[^",]\Qe\E,', event_analysis.event_property_index_to_match_string('e', 1)),
  assert.equals(r'[^",][^,]{1}\Qe\E,', event_analysis.event_property_index_to_match_string('e', 2)),
  assert.equals(
    'pe,',
    REGEXP_EXTRACT('""pe,', event_analysis.event_property_index_to_match_string('e', 1))
  ),
  assert.equals(
    CAST(NULL AS STRING),
    REGEXP_EXTRACT('""pe,', event_analysis.event_property_index_to_match_string('e', 2))
  ),
  assert.equals(
    CAST(NULL AS STRING),
    REGEXP_EXTRACT('"p"e,', event_analysis.event_property_index_to_match_string('e', 1))
  ),
  assert.equals(
    'p"e,',
    REGEXP_EXTRACT('"p"e,', event_analysis.event_property_index_to_match_string('e', 2))
  ),
