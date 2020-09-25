CREATE OR REPLACE VIEW
  product_events
AS
SELECT
  created AS event_timestamp,
  `data`.product.*,
FROM
  stripe_external.events_v1
WHERE
  `data`.product IS NOT NULL
UNION ALL
SELECT
  created AS event_timestamp,
  *,
FROM
  stripe_external.products_v1
