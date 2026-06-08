-- Ingestion payload APPENDS to a nested array-of-objects custom attribute
-- (fxa_services) and subscribes the user to the service's Braze transactional subscription group.
--
-- Scope: we only sync services that map to a Braze subscription group.
--
-- De-duplication: Braze's `add` operator ALWAYS appends (it does not upsert or
-- dedupe -- $identifier_key only applies to `update`/`remove`), so duplicates are
-- prevented solely by anti-joining against this table's own history (already_synced)
-- and emitting each (uid, service) at most once. This table must therefore keep
-- full history and is NOT expired. NOTE: re-backfilling or truncating this table
-- would re-emit `add`s and create duplicate array entries in Braze -- treat its
-- history as load-bearing.
--
-- Email/subscription semantics: the mapped service's emails are TRANSACTIONAL
-- service emails, routed via the service's subscription group.
-- We set the per-group subscription_state to 'subscribed' (delivery routing). We do
-- NOT set the profile-level email_subscribe, which is the user's GLOBAL email
-- master-switch.
WITH service_subscription_groups AS (
  SELECT
    'smartwindow' AS service,
    '38994d30-52ea-4729-b6b3-255e05115db5' AS subscription_group_id
  -- To onboard a new service, add a row here:
  -- UNION ALL
  -- SELECT '<service>', '<subscription-group-id>'
),
already_synced AS (
  SELECT DISTINCT
    ALIAS_NAME AS uid,
    JSON_VALUE(service_element, '$.service') AS service
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_fxa_services_sync_v1`,
    UNNEST(JSON_QUERY_ARRAY(payload.fxa_services["add"])) AS service_element
),
new_services AS (
  SELECT
    users.uid,
    users.service,
    ANY_VALUE(users.email) AS email,
    ANY_VALUE(service_mapping.subscription_group_id) AS subscription_group_id,
    MIN(users.first_authorized_tos_at) AS first_authorized_tos_at
  FROM
    `moz-fx-data-shared-prod.braze_derived.fxa_services_v1` AS users
  INNER JOIN
    service_subscription_groups AS service_mapping
    USING (service)
  LEFT JOIN
    already_synced
    ON already_synced.uid = users.uid
    AND already_synced.service = users.service
  WHERE
    users.first_authorized_tos_at IS NOT NULL
    AND already_synced.uid IS NULL
  GROUP BY
    uid,
    service
),
-- aggregate per user here so the final SELECT can UNNEST a plain array column;
-- BigQuery rejects an aggregate (ARRAY_AGG) nested directly inside UNNEST
per_user AS (
  SELECT
    services.uid,
    ANY_VALUE(services.email) AS email,
    -- the objects to append to the user's fxa_services array
    ARRAY_AGG(
      STRUCT(
        services.service AS service,
        STRUCT(
          FORMAT_TIMESTAMP(
            '%Y-%m-%d %H:%M:%E6S UTC',  -- Braze-required nested-timestamp format
            services.first_authorized_tos_at,
            'UTC'
          ) AS `$time`
        ) AS first_authorized_tos_at
      )
      ORDER BY
        services.first_authorized_tos_at  -- ascending: element [0] is the first service
    ) AS services_to_add,
    -- distinct subscription groups for this user's newly synced services
    ARRAY_AGG(DISTINCT services.subscription_group_id) AS subscription_group_ids
  FROM
    new_services AS services
  GROUP BY
    uid
)
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  per_user.uid AS ALIAS_NAME,
  'fxa_id' AS ALIAS_LABEL,
  JSON_STRIP_NULLS(
    TO_JSON(
      STRUCT(
        per_user.email AS email,
        STRUCT(per_user.services_to_add AS `add`) AS fxa_services,
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
