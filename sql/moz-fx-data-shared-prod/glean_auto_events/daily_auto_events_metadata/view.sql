CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glean_auto_events.daily_auto_events_metadata`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.glean_auto_events_derived.daily_auto_events_metadata_v1`
