-- Braze Cloud Data Ingestion payload for FxA services mapped to a subscription
-- group (today just 'smartwindow'). fxa_services is sent as a PLAIN ARRAY (not the
-- `add` operator, which CDI collapses to a single object). We anti-join against this
-- table's own history (already_synced) to find users with any mapped service not yet
-- synced -- timestamp-independent, so onboarding a service backfills its existing
-- authorizers and late/backdated rows aren't dropped -- then emit each such user's
-- FULL mapped set so the overwrite rebuilds the array and keeps prior services.
-- Requires full history (NOT expired). To onboard a service, add a row to
-- service_subscription_groups (keep in sync with changed_fxa_services_events_sync_v1).
-- Note: rows written before the plain-array change stored fxa_services as
-- {"add": [...]}, which JSON_QUERY_ARRAY reads as empty, so the first run after
-- deploy re-emits every user once to migrate them to the array shape.
WITH service_subscription_groups AS (
  SELECT
    'smartwindow' AS service,
    '38994d30-52ea-4729-b6b3-255e05115db5' AS subscription_group_id
),
already_synced AS (
  SELECT DISTINCT
    ALIAS_NAME AS uid,
    JSON_VALUE(service_element, '$.service') AS service
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_fxa_services_sync_v1`,
    UNNEST(JSON_QUERY_ARRAY(payload.fxa_services)) AS service_element
),
users_with_new_service AS (
  SELECT DISTINCT
    users.uid
  FROM
    `moz-fx-data-shared-prod.braze_derived.fxa_services_v1` AS users
  INNER JOIN
    service_subscription_groups
    USING (service)
  LEFT JOIN
    already_synced
    ON already_synced.uid = users.uid
    AND already_synced.service = users.service
  WHERE
    users.first_authorized_tos_at IS NOT NULL
    AND already_synced.uid IS NULL
),
per_user AS (
  SELECT
    users.uid,
    ANY_VALUE(users.email) AS email,
    ARRAY_AGG(
      STRUCT(
        users.service AS service,
        STRUCT(
          FORMAT_TIMESTAMP(
            '%Y-%m-%d %H:%M:%E6S UTC',
            users.first_authorized_tos_at,
            'UTC'
          ) AS `$time`
        ) AS first_authorized_tos_at
      )
      ORDER BY
        users.first_authorized_tos_at
    ) AS fxa_services,
    ARRAY_AGG(DISTINCT service_mapping.subscription_group_id) AS subscription_group_ids
  FROM
    `moz-fx-data-shared-prod.braze_derived.fxa_services_v1` AS users
  INNER JOIN
    service_subscription_groups AS service_mapping
    USING (service)
  INNER JOIN
    users_with_new_service
    USING (uid)
  WHERE
    users.first_authorized_tos_at IS NOT NULL
  GROUP BY
    users.uid
)
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  per_user.uid AS ALIAS_NAME,
  'fxa_id' AS ALIAS_LABEL,
  JSON_STRIP_NULLS(
    TO_JSON(
      STRUCT(
        per_user.email AS email,
        per_user.fxa_services AS fxa_services,
        ARRAY(
          SELECT AS STRUCT
            group_id AS subscription_group_id,
            'subscribed' AS subscription_state
          FROM
            UNNEST(per_user.subscription_group_ids) AS group_id
        ) AS subscription_groups
      )
    )
  ) AS PAYLOAD
FROM
  per_user;
