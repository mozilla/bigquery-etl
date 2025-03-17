WITH subscription_starts AS (
  SELECT
    CONCAT(
      subscription.provider,
      '-',
      subscription.provider_subscription_id,
      '-',
      service.id,
      '-',
      FORMAT_TIMESTAMP('%FT%H:%M:%E6S', valid_from)
    ) AS subscription_id,
    subscription.id AS logical_subscription_id,
    subscription.services,
    service.id AS service_id,
    valid_from AS started_at,
    subscription.mozilla_account_id_sha256,
    subscription.provider_customer_id,
    subscription.provider_subscription_id,
    IF(
      valid_from = subscription.started_at,
      STRUCT(subscription.initial_discount_name, subscription.initial_discount_promotion_code),
      STRUCT(
        subscription.current_period_discount_name AS initial_discount_name,
        subscription.current_period_discount_promotion_code AS initial_discount_promotion_code
      )
    ).*
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.logical_subscriptions_history_v1`
  CROSS JOIN
    UNNEST(subscription.services) AS service
  QUALIFY
    valid_from IS DISTINCT FROM LAG(valid_to) OVER (
      PARTITION BY
        subscription.id,
        service.id
      ORDER BY
        valid_from
    )
),
subscriptions_history_periods AS (
  SELECT
    subscription_id,
    logical_subscription_id,
    service_id,
    started_at,
    COALESCE(
      LEAD(started_at) OVER (PARTITION BY logical_subscription_id, service_id ORDER BY started_at),
      '9999-12-31 23:59:59.999999'
    ) AS ended_at,
    ROW_NUMBER() OVER (
      PARTITION BY
        -- We don't have unhashed Mozilla Account IDs for some historical customers, so we use the hashed IDs instead,
        -- and if we don't have any Mozilla Account ID data we fall back to the provider's customer/subscription IDs.
        COALESCE(mozilla_account_id_sha256, provider_customer_id, provider_subscription_id),
        service_id
      ORDER BY
        started_at,
        subscription_id
    ) AS customer_service_subscription_number,
    initial_discount_name,
    initial_discount_promotion_code
  FROM
    subscription_starts
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
    `moz-fx-data-shared-prod.subscription_platform_derived.subplat_attribution_impressions_v1`
  CROSS JOIN
    UNNEST(mozilla_account_ids_sha256) AS mozilla_account_id_sha256
  UNION ALL
  -- Include historical VPN attributions from before VPN's SubPlat funnel was implemented on 2021-08-25.
  SELECT
    users.fxa_uid AS mozilla_account_id_sha256,
    users.created_at AS impression_at,
    CAST(NULL AS STRING) AS entrypoint,
    users_attribution.attribution.entrypoint_experiment,
    users_attribution.attribution.entrypoint_variation,
    users_attribution.attribution.utm_campaign,
    users_attribution.attribution.utm_content,
    users_attribution.attribution.utm_medium,
    users_attribution.attribution.utm_source,
    users_attribution.attribution.utm_term,
    ['VPN'] AS service_ids
  FROM
    `moz-fx-data-shared-prod.mozilla_vpn_derived.users_attribution_v1` AS users_attribution
  JOIN
    `moz-fx-data-shared-prod.mozilla_vpn_derived.users_v1` AS users
    ON users_attribution.user_id = users.id
  WHERE
    DATE(users.created_at) <= '2021-08-25'
    AND (
      users_attribution.attribution.entrypoint_experiment IS NOT NULL
      OR users_attribution.attribution.entrypoint_variation IS NOT NULL
      OR users_attribution.attribution.utm_campaign IS NOT NULL
      OR users_attribution.attribution.utm_content IS NOT NULL
      OR users_attribution.attribution.utm_medium IS NOT NULL
      OR users_attribution.attribution.utm_source IS NOT NULL
      OR users_attribution.attribution.utm_term IS NOT NULL
    )
),
subscription_attributions AS (
  SELECT
    subscription_starts.subscription_id,
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
  GROUP BY
    subscription_id
),
subscriptions_history AS (
  SELECT
    CONCAT(
      subscriptions_history_periods.subscription_id,
      '-',
      FORMAT_TIMESTAMP('%FT%H:%M:%E6S', history.valid_from)
    ) AS id,
    history.valid_from,
    history.valid_to,
    history.id AS logical_subscriptions_history_id,
    STRUCT(
      subscriptions_history_periods.subscription_id AS id,
      history.subscription.provider,
      history.subscription.payment_provider,
      history.subscription.id AS logical_subscription_id,
      history.subscription.provider_subscription_id,
      history.subscription.provider_subscription_item_id,
      history.subscription.provider_subscription_created_at,
      history.subscription.provider_subscription_updated_at,
      history.subscription.provider_customer_id,
      history.subscription.mozilla_account_id,
      history.subscription.mozilla_account_id_sha256,
      history.subscription.customer_subscription_number AS customer_logical_subscription_number,
      subscriptions_history_periods.customer_service_subscription_number,
      history.subscription.country_code,
      history.subscription.country_name,
      service,
      ARRAY(
        SELECT
          other_service
        FROM
          UNNEST(history.subscription.services) AS other_service
        WHERE
          other_service.id != service.id
        ORDER BY
          other_service.id
      ) AS other_services,
      history.subscription.provider_product_id,
      history.subscription.product_name,
      history.subscription.provider_plan_id,
      history.subscription.plan_summary,
      history.subscription.plan_interval,
      history.subscription.plan_interval_type,
      history.subscription.plan_interval_count,
      history.subscription.plan_interval_months,
      history.subscription.plan_currency,
      history.subscription.plan_amount,
      history.subscription.is_bundle,
      history.subscription.is_trial,
      history.subscription.is_active,
      history.subscription.provider_status,
      history.subscription.started_at AS logical_subscription_started_at,
      subscriptions_history_periods.started_at,
      history.subscription.ended_at,
      history.subscription.current_period_started_at,
      history.subscription.current_period_ends_at,
      history.subscription.auto_renew,
      history.subscription.auto_renew_disabled_at,
      subscriptions_history_periods.initial_discount_name,
      subscriptions_history_periods.initial_discount_promotion_code,
      history.subscription.current_period_discount_name,
      history.subscription.current_period_discount_promotion_code,
      history.subscription.current_period_discount_amount,
      history.subscription.ongoing_discount_name,
      history.subscription.ongoing_discount_promotion_code,
      history.subscription.ongoing_discount_amount,
      history.subscription.ongoing_discount_ends_at,
      history.subscription.has_refunds,
      history.subscription.has_fraudulent_charges,
      subscription_attributions.first_touch_attribution,
      subscription_attributions.last_touch_attribution
    ) AS subscription
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.logical_subscriptions_history_v1` AS history
  CROSS JOIN
    UNNEST(history.subscription.services) AS service
  JOIN
    subscriptions_history_periods
    ON history.subscription.id = subscriptions_history_periods.logical_subscription_id
    AND service.id = subscriptions_history_periods.service_id
    AND history.valid_from >= subscriptions_history_periods.started_at
    AND history.valid_from < subscriptions_history_periods.ended_at
  LEFT JOIN
    subscription_attributions
    ON subscriptions_history_periods.subscription_id = subscription_attributions.subscription_id
),
synthetic_subscription_ends_history AS (
  -- Synthesize subscription end history records if subscriptions get downgraded to no longer include a service.
  SELECT
    CONCAT(subscription.id, '-', FORMAT_TIMESTAMP('%FT%H:%M:%E6S', valid_to)) AS id,
    valid_to AS valid_from,
    TIMESTAMP('9999-12-31 23:59:59.999999') AS valid_to,
    logical_subscriptions_history_id,
    (
      SELECT AS STRUCT
        subscription.* REPLACE (
          FALSE AS is_active,
          valid_to AS ended_at,
          CAST(NULL AS TIMESTAMP) AS current_period_started_at,
          CAST(NULL AS TIMESTAMP) AS current_period_ends_at
        )
    ) AS subscription
  FROM
    subscriptions_history
  QUALIFY
    1 = ROW_NUMBER() OVER (PARTITION BY subscription.id ORDER BY valid_from DESC)
    AND valid_to < '9999-12-31 23:59:59.999999'
)
SELECT
  *
FROM
  subscriptions_history
UNION ALL
SELECT
  *
FROM
  synthetic_subscription_ends_history
