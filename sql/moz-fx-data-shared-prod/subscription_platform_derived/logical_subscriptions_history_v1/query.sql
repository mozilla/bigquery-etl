WITH history AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.stripe_logical_subscriptions_history_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.google_logical_subscriptions_history_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.apple_logical_subscriptions_history_v1`
),
countries AS (
  SELECT
    code,
    name
  FROM
    `moz-fx-data-shared-prod.static.country_codes_v1`
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
),
subscription_starts AS (
  SELECT
    subscription.id AS subscription_id,
    subscription.provider,
    subscription.started_at,
    subscription.mozilla_account_id_sha256,
    subscription.services
  FROM
    history
  QUALIFY
    1 = ROW_NUMBER() OVER (PARTITION BY subscription.id ORDER BY valid_from, valid_to)
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
  WHERE
    -- The SubPlat attribution impression events we have access to are only for the Stripe subscription funnel.
    subscription_starts.provider = 'Stripe'
  GROUP BY
    subscription_id
)
SELECT
  history.id,
  history.valid_from,
  history.valid_to,
  history.provider_subscriptions_history_id,
  STRUCT(
    history.subscription.id,
    history.subscription.provider,
    history.subscription.payment_provider,
    history.subscription.provider_subscription_id,
    history.subscription.provider_subscription_item_id,
    history.subscription.provider_subscription_created_at,
    history.valid_from AS provider_subscription_updated_at,
    history.subscription.provider_customer_id,
    history.subscription.mozilla_account_id,
    history.subscription.mozilla_account_id_sha256,
    DENSE_RANK() OVER (
      PARTITION BY
        -- We don't have unhashed Mozilla Account IDs for some historical customers, so we use the hashed IDs instead,
        -- and if we don't have any Mozilla Account ID data we fall back to the provider's customer/subscription IDs.
        COALESCE(
          history.subscription.mozilla_account_id_sha256,
          history.subscription.provider_customer_id,
          history.subscription.provider_subscription_id
        )
      ORDER BY
        history.subscription.started_at,
        history.subscription.id
    ) AS customer_subscription_number,
    history.subscription.country_code,
    COALESCE(countries.name, history.subscription.country_code, 'Unknown') AS country_name,
    history.subscription.services,
    history.subscription.provider_product_id,
    history.subscription.product_name,
    history.subscription.provider_plan_id,
    CONCAT(
      history.subscription.plan_interval_count,
      ' ',
      history.subscription.plan_interval_type,
      IF(history.subscription.plan_interval_count > 1, 's', ''),
      IF(
        history.subscription.plan_amount IS NOT NULL,
        CONCAT(
          ' ',
          history.subscription.plan_currency,
          ' ',
          FORMAT('%.2f', history.subscription.plan_amount)
        ),
        ''
      ),
      IF(history.subscription.is_bundle, ' bundle', '')
    ) AS plan_summary,
    CONCAT(
      history.subscription.plan_interval_count,
      ' ',
      history.subscription.plan_interval_type,
      IF(history.subscription.plan_interval_count > 1, 's', '')
    ) AS plan_interval,
    history.subscription.plan_interval_type,
    history.subscription.plan_interval_count,
    CASE
      history.subscription.plan_interval_type
      WHEN 'month'
        THEN history.subscription.plan_interval_count
      WHEN 'year'
        THEN history.subscription.plan_interval_count * 12
    END AS plan_interval_months,
    history.subscription.plan_currency,
    history.subscription.plan_amount,
    history.subscription.is_bundle,
    history.subscription.is_trial,
    history.subscription.is_active,
    history.subscription.provider_status,
    history.subscription.started_at,
    history.subscription.ended_at,
    history.subscription.current_period_started_at,
    history.subscription.current_period_ends_at,
    history.subscription.auto_renew,
    history.subscription.auto_renew_disabled_at,
    history.subscription.initial_discount_name,
    history.subscription.initial_discount_promotion_code,
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
  history
LEFT JOIN
  countries
  ON history.subscription.country_code = countries.code
LEFT JOIN
  subscription_attributions
  ON history.subscription.id = subscription_attributions.subscription_id
