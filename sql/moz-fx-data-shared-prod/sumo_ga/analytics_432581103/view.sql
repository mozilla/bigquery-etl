CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.sumo_ga.analytics_432581103`
AS
SELECT
  *
FROM
  `moz-fx-data-marketing-prod.analytics_432581103.events_*`
WHERE
  _TABLE_SUFFIX >= '20240503' --first date with data available
