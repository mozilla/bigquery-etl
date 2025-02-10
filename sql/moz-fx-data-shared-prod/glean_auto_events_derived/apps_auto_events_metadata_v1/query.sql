SELECT
  app,
  name,
  SUM(count) count
FROM
  `moz-fx-data-shared-prod.glean_auto_events_derived.daily_auto_events_metadata_v1`
GROUP BY
  app,
  name
