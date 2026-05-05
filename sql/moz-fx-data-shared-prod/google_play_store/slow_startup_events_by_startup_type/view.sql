CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.google_play_store.slow_startup_events_by_startup_type`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.google_play_store_derived.slow_startup_events_by_startup_type_v1`
