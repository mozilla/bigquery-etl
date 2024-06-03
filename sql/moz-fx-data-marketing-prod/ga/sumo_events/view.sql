CREATE OR REPLACE VIEW
  `moz-fx-data-marketing-prod.ga.sumo_events`
AS
SELECT
  *
FROM
  `moz-fx-data-marketing-prod.analytics_314096102.events_*`
WHERE
  _TABLE_SUFFIX >= '20240130' --first date with data available
