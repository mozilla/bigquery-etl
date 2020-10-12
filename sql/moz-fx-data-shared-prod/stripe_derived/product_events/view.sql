CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod`.stripe_derived.product_events
AS
SELECT
  created AS event_timestamp,
  `data`.product.*,
FROM
  `moz-fx-data-shared-prod`.stripe_external.events_v1
WHERE
  `data`.product IS NOT NULL
UNION ALL
SELECT
  created AS event_timestamp,
  *,
FROM
  `moz-fx-data-shared-prod`.stripe_external.products_v1
