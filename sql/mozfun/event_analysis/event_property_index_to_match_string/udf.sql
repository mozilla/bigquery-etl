CREATE OR REPLACE FUNCTION event_analysis.event_property_index_to_match_string(
  property_index INTEGER
)
RETURNS STRING AS (
  CONCAT(
    -- Double-quote characters are used in `events_daily.events` strings to indicate missing properties.
    '[^",]',
    -- Event property values are stored in `events_daily.events` strings in reverse order,
    -- so we expect the Nth property value to be followed by N-1 other property values.
    IF(property_index > 1, CONCAT('[^,]{', (property_index - 1), '}'), '')
  )
);

SELECT
  assert.equals('[^",]', event_analysis.event_property_index_to_match_string(1)),
  assert.equals('[^",][^,]{1}', event_analysis.event_property_index_to_match_string(2)),
  assert.equals(
    'pe,',
    REGEXP_EXTRACT('""pe,', CONCAT(event_analysis.event_property_index_to_match_string(1), 'e,'))
  ),
  assert.equals(
    CAST(NULL AS STRING),
    REGEXP_EXTRACT('""pe,', CONCAT(event_analysis.event_property_index_to_match_string(2), 'e,'))
  ),
  assert.equals(
    CAST(NULL AS STRING),
    REGEXP_EXTRACT('"p"e,', CONCAT(event_analysis.event_property_index_to_match_string(1), 'e,'))
  ),
  assert.equals(
    'p"e,',
    REGEXP_EXTRACT('"p"e,', CONCAT(event_analysis.event_property_index_to_match_string(2), 'e,'))
  ),
