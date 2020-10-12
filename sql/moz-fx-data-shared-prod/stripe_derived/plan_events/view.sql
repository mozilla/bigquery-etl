CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stripe_derived.plan_events`
AS
SELECT
  created AS event_timestamp,
  `data`.plan.*,
FROM
  `moz-fx-data-shared-prod`.stripe_external.events_v1
WHERE
  `data`.plan IS NOT NULL
UNION ALL
SELECT
  created AS event_timestamp,
  *,
FROM
  `moz-fx-data-shared-prod`.stripe_external.plans_v1
