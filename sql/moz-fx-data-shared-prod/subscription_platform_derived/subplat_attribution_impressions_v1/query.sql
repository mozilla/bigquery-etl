{% if is_init() %}
INSERT INTO
 `{project_id}.{dataset_id}.{table_id}`
{% endif %}
WITH services AS (
  SELECT
    id,
    ARRAY(SELECT id FROM UNNEST(subplat_oauth_clients)) AS subplat_oauth_client_ids,
    ARRAY(SELECT name FROM UNNEST(subplat_oauth_clients)) AS subplat_oauth_client_names,
    stripe_product_ids,
    stripe_plan_ids
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.services_v1`
),
new_service_flow_events AS (
  SELECT
    events.flow_id,
    MIN(events.event_time) OVER (PARTITION BY events.flow_id) AS flow_min_event_time,
    events.event_time,
    events.event_type,
    events.entrypoint,
    events.entrypoint_experiment,
    events.entrypoint_variation,
    events.utm_campaign,
    events.utm_content,
    events.utm_medium,
    events.utm_source,
    events.utm_term,
    events.mozilla_account_id_sha256,
    events.oauth_client_id,
    events.oauth_client_name,
    events.product_id,
    events.plan_id,
    services.id AS service_id
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.subplat_flow_events_v1` AS events
  -- If multiple services match this join it can cause fan-outs, but the way we aggregate things this doesn't matter.
  LEFT JOIN
    services
  ON
    events.oauth_client_id IN UNNEST(services.subplat_oauth_client_ids)
    OR events.oauth_client_name IN UNNEST(services.subplat_oauth_client_names)
    -- For a while Bedrock incorrectly passed VPN's OAuth client name as the OAuth client ID.
    OR events.oauth_client_id IN UNNEST(services.subplat_oauth_client_names)
    OR events.product_id IN UNNEST(services.stripe_product_ids)
    OR events.plan_id IN UNNEST(services.stripe_plan_ids)
  WHERE
    {% if is_init() %}
      DATE(events.log_timestamp) < CURRENT_DATE()
    {% else %}
      -- Reprocess the previous day's events as well in case a flow spanned multiple days
      -- but wasn't saved here on the initial day due to not having attribution values yet.
      (DATE(events.log_timestamp) BETWEEN (@date - 1) AND @date)
    {% endif %}
),
service_flow_events AS (
  {% if is_init() %}
    SELECT
      *
    FROM
      new_service_flow_events
  {% else %}
    SELECT
      *
    FROM
      new_service_flow_events
    UNION ALL
    SELECT
      flow_id,
      flow_started_at AS flow_min_event_time,
      impression_at AS event_time,
      event_type,
      entrypoint,
      entrypoint_experiment,
      entrypoint_variation,
      utm_campaign,
      utm_content,
      utm_medium,
      utm_source,
      utm_term,
      mozilla_account_id_sha256,
      oauth_client_id,
      oauth_client_name,
      product_id,
      plan_id,
      service_id
    FROM
      `moz-fx-data-shared-prod.subscription_platform_derived.subplat_attribution_impressions_v1`
    -- Unnesting these arrays can cause fan-outs, but the way we reaggregate things this doesn't matter.
    LEFT JOIN
      UNNEST(mozilla_account_ids_sha256) AS mozilla_account_id_sha256
    LEFT JOIN
      UNNEST(oauth_client_ids) AS oauth_client_id
    LEFT JOIN
      UNNEST(oauth_client_names) AS oauth_client_name
    LEFT JOIN
      UNNEST(product_ids) AS product_id
    LEFT JOIN
      UNNEST(plan_ids) AS plan_id
    LEFT JOIN
      UNNEST(service_ids) AS service_id
  {% endif %}
)
SELECT
  flow_id,
  MIN(flow_min_event_time) AS flow_started_at,
  ARRAY_AGG(
    IF(
      entrypoint_experiment IS NOT NULL
      OR entrypoint_variation IS NOT NULL
      OR utm_campaign IS NOT NULL
      OR utm_content IS NOT NULL
      OR utm_medium IS NOT NULL
      OR utm_source IS NOT NULL
      OR utm_term IS NOT NULL,
      STRUCT(
        event_time AS impression_at,
        event_type,
        entrypoint,
        entrypoint_experiment,
        entrypoint_variation,
        utm_campaign,
        utm_content,
        utm_medium,
        utm_source,
        utm_term
      ),
      NULL
    ) IGNORE NULLS
    ORDER BY
      event_time
    LIMIT
      1
  )[ORDINAL(1)].*,
  ARRAY_AGG(
    DISTINCT mozilla_account_id_sha256 IGNORE NULLS
    ORDER BY
      mozilla_account_id_sha256
  ) AS mozilla_account_ids_sha256,
  ARRAY_AGG(DISTINCT oauth_client_id IGNORE NULLS ORDER BY oauth_client_id) AS oauth_client_ids,
  ARRAY_AGG(
    DISTINCT oauth_client_name IGNORE NULLS
    ORDER BY
      oauth_client_name
  ) AS oauth_client_names,
  ARRAY_AGG(DISTINCT product_id IGNORE NULLS ORDER BY product_id) AS product_ids,
  ARRAY_AGG(DISTINCT plan_id IGNORE NULLS ORDER BY plan_id) AS plan_ids,
  ARRAY_AGG(DISTINCT service_id IGNORE NULLS ORDER BY service_id) AS service_ids
FROM
  service_flow_events AS events
GROUP BY
  flow_id
HAVING
  LOGICAL_OR(
    events.entrypoint_experiment IS NOT NULL
    OR events.entrypoint_variation IS NOT NULL
    OR events.utm_campaign IS NOT NULL
    OR events.utm_content IS NOT NULL
    OR events.utm_medium IS NOT NULL
    OR events.utm_source IS NOT NULL
    OR events.utm_term IS NOT NULL
  )
