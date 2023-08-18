WITH new_flow_events AS (
  SELECT
    logger,
    `timestamp` AS log_timestamp,
    COALESCE(event_time, `timestamp`) AS event_time,
    event_type,
    flow_id,
    user_id AS mozilla_account_id_sha256,
    oauth_client_id,
    service AS oauth_client_name,
    checkout_type,
    payment_provider,
    subscription_id,
    product_id,
    plan_id,
    entrypoint,
    entrypoint_experiment,
    entrypoint_variation,
    utm_campaign,
    utm_content,
    utm_medium,
    utm_source,
    utm_term,
    promotion_code,
    country_code_source,
    country_code,
    country,
    `language`,
    os_name,
    os_version,
    ua_browser,
    ua_version,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts.fxa_all_events`
  WHERE
    fxa_log IN ('content', 'auth', 'stdout')
    AND flow_id IS NOT NULL
    AND DATE(`timestamp`) = @date
),
services_metadata AS (
  SELECT
    ARRAY_AGG(DISTINCT subplat_oauth_client.id IGNORE NULLS) AS subplat_oauth_client_ids,
    ARRAY_AGG(DISTINCT subplat_oauth_client.name IGNORE NULLS) AS subplat_oauth_client_names
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.services_v1`
  LEFT JOIN
    UNNEST(subplat_oauth_clients) AS subplat_oauth_client
),
existing_flow_ids AS (
  SELECT DISTINCT
    flow_id
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.subplat_flow_events_v1`
)
SELECT
  new_flow_events.*
FROM
  new_flow_events
CROSS JOIN
  services_metadata
LEFT JOIN
  existing_flow_ids
ON
  new_flow_events.flow_id = existing_flow_ids.flow_id
QUALIFY
  LOGICAL_OR(
    new_flow_events.oauth_client_id IN UNNEST(services_metadata.subplat_oauth_client_ids)
    OR new_flow_events.oauth_client_name IN UNNEST(services_metadata.subplat_oauth_client_names)
    -- For a while Bedrock incorrectly passed VPN's OAuth client name as the OAuth client ID.
    OR new_flow_events.oauth_client_id IN UNNEST(services_metadata.subplat_oauth_client_names)
    OR new_flow_events.subscription_id IS NOT NULL
    OR new_flow_events.product_id IS NOT NULL
    OR new_flow_events.plan_id IS NOT NULL
  ) OVER (PARTITION BY new_flow_events.flow_id)
  OR existing_flow_ids.flow_id IS NOT NULL
