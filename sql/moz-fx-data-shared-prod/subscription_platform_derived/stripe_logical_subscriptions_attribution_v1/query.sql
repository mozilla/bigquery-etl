WITH subscription_starts AS (
  SELECT
    subscription.id AS subscription_id,
    subscription.started_at,
    subscription.mozilla_account_id_sha256,
    subscription.services
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.stripe_logical_subscriptions_history_v1`
  WHERE
    {% if is_init() %}
      DATE(subscription.started_at) <= DATE(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
    {% else %}
      DATE(subscription.started_at) = @date
    {% endif %}
  QUALIFY
    1 = ROW_NUMBER() OVER (PARTITION BY subscription.id ORDER BY valid_from, valid_to)
),
customer_attribution_impressions AS (
  SELECT
    mozilla_account_id_sha256,
    impression_at,
    entrypoint,
    entrypoint_experiment,
    entrypoint_variation,
    utm_campaign,
    utm_content,
    utm_medium,
    utm_source,
    utm_term,
    service_ids
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.recent_subplat_attribution_impressions_v1`
  CROSS JOIN
    UNNEST(mozilla_account_ids_sha256) AS mozilla_account_id_sha256
  UNION ALL
  SELECT
    mozilla_account_id_sha256,
    impression_at,
    entrypoint,
    entrypoint_experiment,
    entrypoint_variation,
    utm_campaign,
    utm_content,
    utm_medium,
    utm_source,
    utm_term,
    service_ids
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.subplat_attribution_impressions_v1`
  CROSS JOIN
    UNNEST(mozilla_account_ids_sha256) AS mozilla_account_id_sha256
  WHERE
    DATE(impression_at) < (
      SELECT
        COALESCE(MIN(DATE(impression_at)), '9999-12-31')
      FROM
        `moz-fx-data-shared-prod.subscription_platform_derived.recent_subplat_attribution_impressions_v1`
    )
  UNION ALL
  -- Include historical VPN attributions from before VPN's SubPlat funnel was implemented on 2021-08-25.
  SELECT
    fxa_uid AS mozilla_account_id_sha256,
    user_created_at AS impression_at,
    CAST(NULL AS STRING) AS entrypoint,
    attribution.entrypoint_experiment,
    attribution.entrypoint_variation,
    attribution.utm_campaign,
    attribution.utm_content,
    attribution.utm_medium,
    attribution.utm_source,
    attribution.utm_term,
    ['VPN'] AS service_ids
  FROM
    `moz-fx-data-shared-prod.mozilla_vpn_derived.users_attribution_v2`
  WHERE
    fxa_uid IS NOT NULL
    AND DATE(user_created_at) <= '2021-08-25'
    AND (
      attribution.entrypoint_experiment IS NOT NULL
      OR attribution.entrypoint_variation IS NOT NULL
      OR attribution.utm_campaign IS NOT NULL
      OR attribution.utm_content IS NOT NULL
      OR attribution.utm_medium IS NOT NULL
      OR attribution.utm_source IS NOT NULL
      OR attribution.utm_term IS NOT NULL
    )
)
SELECT
  subscription_starts.subscription_id,
  ANY_VALUE(subscription_starts.started_at) AS subscription_started_at,
  MIN_BY(
    STRUCT(
      customer_attribution_impressions.impression_at,
      customer_attribution_impressions.entrypoint,
      customer_attribution_impressions.entrypoint_experiment,
      customer_attribution_impressions.entrypoint_variation,
      customer_attribution_impressions.utm_campaign,
      customer_attribution_impressions.utm_content,
      customer_attribution_impressions.utm_medium,
      customer_attribution_impressions.utm_source,
      customer_attribution_impressions.utm_term
      -- TODO: calculate normalized attribution values like `mozfun.norm.vpn_attribution()` does
    ),
    customer_attribution_impressions.impression_at
  ) AS first_touch_attribution,
  MAX_BY(
    STRUCT(
      customer_attribution_impressions.impression_at,
      customer_attribution_impressions.entrypoint,
      customer_attribution_impressions.entrypoint_experiment,
      customer_attribution_impressions.entrypoint_variation,
      customer_attribution_impressions.utm_campaign,
      customer_attribution_impressions.utm_content,
      customer_attribution_impressions.utm_medium,
      customer_attribution_impressions.utm_source,
      customer_attribution_impressions.utm_term
      -- TODO: calculate normalized attribution values like `mozfun.norm.vpn_attribution()` does
    ),
    customer_attribution_impressions.impression_at
  ) AS last_touch_attribution
FROM
  subscription_starts
CROSS JOIN
  UNNEST(subscription_starts.services) AS service
JOIN
  customer_attribution_impressions
  ON subscription_starts.mozilla_account_id_sha256 = customer_attribution_impressions.mozilla_account_id_sha256
  AND service.id IN UNNEST(customer_attribution_impressions.service_ids)
  AND subscription_starts.started_at >= customer_attribution_impressions.impression_at
WHERE
  -- Require at least one of the core attribution values to be set, and exclude some nonsensical attribution values (DENG-9776).
  (
    customer_attribution_impressions.entrypoint_experiment IS NOT NULL
    OR NULLIF(customer_attribution_impressions.utm_campaign, 'invalid') IS NOT NULL
    OR NULLIF(customer_attribution_impressions.utm_source, 'invalid') IS NOT NULL
  )
  AND (
    customer_attribution_impressions.utm_campaign IN (
      'download-client',
      'fx-forgot-password',
      'subscription-download',
      'FuckOff'
    )
  ) IS NOT TRUE
GROUP BY
  subscription_id
