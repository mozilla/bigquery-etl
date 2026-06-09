-- Braze Cloud Data Ingestion payload for FxA services mapped to a subscription
-- group (today just 'smartwindow'). fxa_services is sent as a PLAIN ARRAY (not the
-- `add` operator, which CDI collapses to a single object). We send each user's full
-- mapped set so the overwrite rebuilds the array and keeps prior services; the
-- watermark gates whole users (HAVING), since first_authorized_tos_at is immutable.
-- To onboard a service, add a row to service_subscription_groups (keep in sync with
-- changed_fxa_services_events_sync_v1).
WITH service_subscription_groups AS (
  SELECT
    'smartwindow' AS service,
    '38994d30-52ea-4729-b6b3-255e05115db5' AS subscription_group_id
),
max_update AS (
  SELECT
    MAX(
      TIMESTAMP(JSON_VALUE(service_element, '$.first_authorized_tos_at."$time"'))
    ) AS latest_first_authorized_tos_at
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_fxa_services_sync_v1`,
    UNNEST(JSON_QUERY_ARRAY(payload.fxa_services)) AS service_element
),
per_user AS (
  SELECT
    users.uid,
    ANY_VALUE(users.email) AS email,
    ARRAY_AGG(
      STRUCT(
        users.service AS service,
        STRUCT(
          FORMAT_TIMESTAMP(  -- Braze-required nested-timestamp format
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
  WHERE
    users.first_authorized_tos_at IS NOT NULL  -- null timestamps error in Braze
  GROUP BY
    users.uid
  HAVING
    MAX(users.first_authorized_tos_at) > COALESCE(
      (SELECT latest_first_authorized_tos_at FROM max_update),
      TIMESTAMP('2020-01-01')
    )
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
