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
subscription_starts AS (
  SELECT
    history.subscription.id AS subscription_id,
    history.subscription.started_at,
    history.subscription.is_trial AS started_as_trial,
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
    ) AS customer_subscription_number
  FROM
    history
  QUALIFY
    1 = ROW_NUMBER() OVER (
      PARTITION BY
        history.subscription.id
      ORDER BY
        history.valid_from,
        history.valid_to
    )
),
subscription_attributions AS (
  SELECT
    subscription_id,
    IF(
      attribution_v2.subscription_id IS NOT NULL,
      NULL,
      attribution_v1.first_touch_attribution
    ) AS first_touch_attribution,
    COALESCE(
      attribution_v2.last_touch_attribution,
      attribution_v1.last_touch_attribution
    ) AS last_touch_attribution
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.stripe_logical_subscriptions_attribution_v1` AS attribution_v1
  FULL JOIN
    `moz-fx-data-shared-prod.subscription_platform_derived.stripe_logical_subscriptions_attribution_v2` AS attribution_v2
    USING (subscription_id)
),
subscription_attributions_with_channel AS (
  SELECT
    subscription_id,
    first_touch_attribution,
    CASE
      WHEN last_touch_attribution IS NULL
        THEN NULL
      ELSE (
          SELECT AS STRUCT
            last_touch_attribution.*,
            mozfun.norm.subplat_attribution_channel_group(
              last_touch_attribution.utm_source
            ) AS channel_group
        )
    END AS last_touch_attribution
  FROM
    subscription_attributions
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
    subscription_starts.customer_subscription_number,
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
    history.subscription.has_refunds,
    history.subscription.has_fraudulent_charges,
    subscription_attributions_with_channel.first_touch_attribution,
    subscription_attributions_with_channel.last_touch_attribution,
    history.subscription.initial_discount_name,
    history.subscription.initial_discount_promotion_code,
    history.subscription.current_period_discount_name,
    history.subscription.current_period_discount_promotion_code,
    history.subscription.current_period_discount_amount,
    history.subscription.ongoing_discount_name,
    history.subscription.ongoing_discount_promotion_code,
    history.subscription.ongoing_discount_amount,
    history.subscription.ongoing_discount_ends_at,
    history.subscription.ended_reason,
    CONCAT(
      IF(
        subscription_starts.customer_subscription_number = 1,
        'New Customer',
        'Returning Customer'
      ),
      IF(subscription_starts.started_as_trial, ' Trial', '')
    ) AS started_reason,
    history.subscription.payment_method
  ) AS subscription
FROM
  history
INNER JOIN
  subscription_starts
  ON history.subscription.id = subscription_starts.subscription_id
LEFT JOIN
  countries
  ON history.subscription.country_code = countries.code
LEFT JOIN
  subscription_attributions_with_channel
  ON history.subscription.id = subscription_attributions_with_channel.subscription_id
