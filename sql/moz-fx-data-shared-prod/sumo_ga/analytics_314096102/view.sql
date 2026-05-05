CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.sumo_ga.analytics_314096102`
AS
SELECT
  *
FROM
  `moz-fx-data-marketing-prod.analytics_314096102.events_*`
WHERE
  _TABLE_SUFFIX >= '20240130' --first date with data available
