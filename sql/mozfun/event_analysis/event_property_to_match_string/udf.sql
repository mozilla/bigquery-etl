CREATE OR REPLACE FUNCTION event_analysis.event_property_to_match_string(property_index INTEGER)
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
  assert.equals('[^",]', event_analysis.event_property_to_match_string(1)),
  assert.equals('[^",][^,]{1}', event_analysis.event_property_to_match_string(2)),
