-- Braze custom-event payloads recording a "<service> - subscribe" event the first
-- time a user authorizes a service that maps to a Braze email subscription group.
-- Modeled on Basket's per-newsletter "<name> - subscribe" custom event (see
-- mozmeao/basket basket/news/backends/braze.py).
--
-- The mapped service's emails are transactional welcome communications; this event marks
-- the start of the user's use of the service so Braze can time those transactional sends.
--
-- To onboard a new service, add a row to service_subscription_groups below (keep
-- in sync with the same CTE in changed_fxa_services_sync_v1).
WITH service_subscription_groups AS (
  SELECT
    'smartwindow' AS service,
    '38994d30-52ea-4729-b6b3-255e05115db5' AS subscription_group_id
  -- UNION ALL
  -- SELECT '<service>', '<subscription-group-id>'
),
already_sent AS (
  SELECT DISTINCT
    ALIAS_NAME AS uid,
    JSON_VALUE(payload.name) AS event_name
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_fxa_services_events_sync_v1`
),
new_subscribes AS (
  SELECT DISTINCT
    users.uid,
    CONCAT(users.service, ' - subscribe') AS event_name
  FROM
    `moz-fx-data-shared-prod.braze_derived.fxa_services_v1` AS users
  INNER JOIN
    service_subscription_groups
    USING (service)
  WHERE
    users.first_authorized_tos_at IS NOT NULL
)
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  new_subscribes.uid AS ALIAS_NAME,
  'fxa_id' AS ALIAS_LABEL,
  TO_JSON(STRUCT(new_subscribes.event_name AS name)) AS PAYLOAD
FROM
  new_subscribes
LEFT JOIN
  already_sent
  USING (uid, event_name)
WHERE
  already_sent.uid IS NULL;
