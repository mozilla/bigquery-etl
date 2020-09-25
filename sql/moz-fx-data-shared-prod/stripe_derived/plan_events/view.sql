CREATE OR REPLACE VIEW
  plan_events
AS
SELECT
  created AS event_timestamp,
  `data`.plan.*,
FROM
  stripe_external.events_v1
WHERE
  `data`.plan IS NOT NULL
UNION ALL
SELECT
  created AS event_timestamp,
  *,
FROM
  stripe_external.plans_v1
