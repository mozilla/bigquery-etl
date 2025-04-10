CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glean_auto_events.apps_auto_events_metadata`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.glean_auto_events_derived.apps_auto_events_metadata_v1`
