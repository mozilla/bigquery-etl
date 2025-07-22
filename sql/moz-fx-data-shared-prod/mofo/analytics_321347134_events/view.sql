CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mofo.analytics_321347134_events`
AS
SELECT
  *
FROM
  `moz-fx-data-marketing-prod.analytics_321347134.events_*`
WHERE
  _TABLE_SUFFIX >= '20250719'
