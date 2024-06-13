SELECT
  fxa.email_id AS external_id,
  ARRAY_AGG(
    STRUCT(
      subscriptions.provider AS provider,
      subscriptions.payment_provider AS payment_provider,
      subscriptions.product_name AS product_name,
      services.name AS service_name,
      subscriptions.provider_plan_id AS subscription_plan_id,
      subscriptions.provider_product_id AS subscription_product_id,
      subscriptions.started_at AS subscription_started_at,
      subscriptions.ended_at AS subscription_ended_at,
      subscriptions.plan_summary AS subscription_summary,
      subscriptions.plan_interval_type AS subscription_billing_interval,
      subscriptions.plan_interval_count AS subscription_interval_count,
      subscriptions.plan_currency AS subscription_currency,
      subscriptions.plan_amount AS subscription_amount,
      subscriptions.is_bundle AS is_bundle,
      subscriptions.is_trial AS is_trial,
      subscriptions.is_active AS is_active,
      subscriptions.provider_status AS subscription_status,
      LOWER(subscriptions.country_code) AS subscription_country_code,
      subscriptions.country_name AS subscription_country_name,
      subscriptions.current_period_started_at AS current_period_started_at,
      subscriptions.current_period_ends_at AS current_period_ends_at,
      subscriptions.auto_renew AS is_auto_renew,
      subscriptions.auto_renew_disabled_at AS auto_renew_disabled_at,
      subscriptions.has_refunds AS has_refunds,
      subscriptions.has_fraudulent_charges AS has_fraudulent_charges,
      subscriptions.first_touch_attribution.impression_at AS first_touch_impression_at,
      subscriptions.first_touch_attribution.entrypoint AS first_touch_entrypoint,
      subscriptions.first_touch_attribution.entrypoint_experiment AS first_touch_entrypoint_experiment,
      subscriptions.first_touch_attribution.entrypoint_variation AS first_touch_entrypoint_variation,
      subscriptions.first_touch_attribution.utm_campaign AS first_touch_utm_campaign,
      subscriptions.first_touch_attribution.utm_content AS first_touch_utm_content,
      subscriptions.first_touch_attribution.utm_medium AS first_touch_utm_medium,
      subscriptions.first_touch_attribution.utm_source AS first_touch_utm_source,
      subscriptions.first_touch_attribution.utm_term AS first_touch_utm_term,
      subscriptions.last_touch_attribution.impression_at AS last_touch_impression_at,
      subscriptions.last_touch_attribution.entrypoint AS last_touch_entrypoint,
      subscriptions.last_touch_attribution.entrypoint_experiment AS last_touch_entrypoint_experiment,
      subscriptions.last_touch_attribution.entrypoint_variation AS last_touch_entrypoint_variation,
      subscriptions.last_touch_attribution.utm_campaign AS last_touch_utm_campaign,
      subscriptions.last_touch_attribution.utm_content AS last_touch_utm_content,
      subscriptions.last_touch_attribution.utm_medium AS last_touch_utm_medium,
      subscriptions.last_touch_attribution.utm_source AS last_touch_utm_source,
      subscriptions.last_touch_attribution.utm_term AS last_touch_utm_term,
      subscriptions.provider_subscription_created_at AS subscription_created_at,
      subscriptions.provider_subscription_updated_at AS subscription_updated_at
    )
    ORDER BY
      subscriptions.product_name,
      subscriptions.provider_subscription_updated_at,
      subscriptions.started_at,
      subscriptions.ended_at
  ) AS products
FROM
  `moz-fx-data-shared-prod.subscription_platform.logical_subscriptions` AS subscriptions
LEFT JOIN
  `moz-fx-data-shared-prod.ctms_braze.ctms_fxa` AS fxa
  ON subscriptions.mozilla_account_id_sha256 = TO_HEX(SHA256(fxa.fxa_id))
INNER JOIN
  `moz-fx-data-shared-prod.braze_derived.users_v1` AS users
  ON users.external_id = fxa.email_id
CROSS JOIN
  UNNEST(services) AS services
GROUP BY
  email_id;
