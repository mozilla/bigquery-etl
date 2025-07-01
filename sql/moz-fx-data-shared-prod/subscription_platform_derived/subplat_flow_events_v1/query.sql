WITH fxa_flow_events AS (
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
    `moz-fx-data-shared-prod.firefox_accounts.recent_fxa_all_events`
  WHERE
    fxa_log IN ('content', 'auth', 'stdout', 'payments')
    AND flow_id IS NOT NULL
    {% if not is_init() %}
      -- Include the previous day's events as well in case a flow spanned multiple days.
      AND (DATE(`timestamp`) BETWEEN (@date - 1) AND @date)
    {% endif %}
  UNION ALL
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
    fxa_log IN ('content', 'auth', 'stdout', 'payments')
    AND flow_id IS NOT NULL
    {% if not is_init() %}
      -- Include the previous day's events as well in case a flow spanned multiple days.
      AND (DATE(`timestamp`) BETWEEN (@date - 1) AND @date)
    {% endif %}
    AND DATE(`timestamp`) < (
      SELECT
        MIN(DATE(`timestamp`))
      FROM
        `moz-fx-data-shared-prod.firefox_accounts.recent_fxa_all_events`
    )
),
services_metadata AS (
  SELECT
    ARRAY_AGG(DISTINCT subplat_oauth_client.id IGNORE NULLS) AS subplat_oauth_client_ids,
    ARRAY_AGG(DISTINCT subplat_oauth_client.name IGNORE NULLS) AS subplat_oauth_client_names
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.services_v1`
  LEFT JOIN
    UNNEST(subplat_oauth_clients) AS subplat_oauth_client
)
SELECT
  fxa_flow_events.*
FROM
  fxa_flow_events
CROSS JOIN
  services_metadata
QUALIFY
  DATE(fxa_flow_events.log_timestamp) = @date
  AND LOGICAL_OR(
    fxa_flow_events.oauth_client_id IN UNNEST(services_metadata.subplat_oauth_client_ids)
    OR fxa_flow_events.oauth_client_name IN UNNEST(services_metadata.subplat_oauth_client_names)
    -- For a while Bedrock incorrectly passed VPN's OAuth client name as the OAuth client ID.
    OR fxa_flow_events.oauth_client_id IN UNNEST(services_metadata.subplat_oauth_client_names)
    OR fxa_flow_events.subscription_id IS NOT NULL
    OR fxa_flow_events.product_id IS NOT NULL
    OR fxa_flow_events.plan_id IS NOT NULL
  ) OVER (PARTITION BY fxa_flow_events.flow_id)
