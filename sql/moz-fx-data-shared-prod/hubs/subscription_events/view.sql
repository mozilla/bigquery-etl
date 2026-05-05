CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.hubs.subscription_events`
AS
WITH max_agg_date AS (
  SELECT AS VALUE
    MAX(event_date)
  FROM
    `moz-fx-data-shared-prod`.hubs_derived.subscription_events_v1
)
SELECT
  subscription_events_live.*
FROM
  `moz-fx-data-shared-prod`.hubs_derived.subscription_events_live
CROSS JOIN
  max_agg_date
WHERE
  event_date > max_agg_date
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.hubs_derived.subscription_events_v1
