WITH max_update AS (
  SELECT
    MAX(UPDATED_AT) AS max_update_timestamp
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_products_sync_v1`
  LIMIT
    1
)
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  products.external_id AS EXTERNAL_ID,
  TO_JSON(
    STRUCT(
      ARRAY_AGG(
        STRUCT(
          products_array.provider AS provider,
          products_array.payment_provider AS payment_provider,
          products_array.product_name AS product_name,
          products_array.service_name AS service_name,
          products_array.subscription_plan_id AS subscription_plan_id,
          products_array.subscription_product_id AS subscription_product_id,
          products_array.subscription_started_at AS subscription_started_at,
          products_array.subscription_ended_at AS subscription_ended_at,
          products_array.subscription_summary AS subscription_summary,
          products_array.subscription_billing_interval AS subscription_billing_interval,
          products_array.subscription_interval_count AS subscription_interval_count,
          products_array.subscription_currency AS subscription_currency,
          products_array.subscription_amount AS subscription_amount,
          products_array.is_bundle AS is_bundle,
          products_array.is_trial AS is_trial,
          products_array.is_active AS is_active,
          products_array.subscription_status AS subscription_status,
          products_array.subscription_country_code AS subscription_country_code,
          products_array.subscription_country_name AS subscription_country_name,
          products_array.current_period_started_at AS current_period_started_at,
          products_array.current_period_ends_at AS current_period_ends_at,
          products_array.is_auto_renew AS is_auto_renew,
          products_array.auto_renew_disabled_at AS auto_renew_disabled_at,
          products_array.has_refunds AS has_refunds,
          products_array.has_fraudulent_charges AS has_fraudulent_charges,
          products_array.active_subscriptions_count AS active_subscriptions_count,
          products_array.first_touch_impression_at AS first_touch_impression_at,
          products_array.first_touch_entrypoint AS first_touch_entrypoint,
          products_array.first_touch_entrypoint_experiment AS first_touch_entrypoint_experiment,
          products_array.first_touch_entrypoint_variation AS first_touch_entrypoint_variation,
          products_array.first_touch_utm_campaign AS first_touch_utm_campaign,
          products_array.first_touch_utm_content AS first_touch_utm_content,
          products_array.first_touch_utm_medium AS first_touch_utm_medium,
          products_array.first_touch_utm_source AS first_touch_utm_source,
          products_array.first_touch_utm_term AS first_touch_utm_term,
          products_array.last_touch_impression_at AS last_touch_impression_at,
          products_array.last_touch_entrypoint AS last_touch_entrypoint,
          products_array.last_touch_entrypoint_experiment AS last_touch_entrypoint_experiment,
          products_array.last_touch_entrypoint_variation AS last_touch_entrypoint_variation,
          products_array.last_touch_utm_campaign AS last_touch_utm_campaign,
          products_array.last_touch_utm_content AS last_touch_utm_content,
          products_array.last_touch_utm_medium AS last_touch_utm_medium,
          products_array.last_touch_utm_source AS last_touch_utm_source,
          products_array.last_touch_utm_term AS last_touch_utm_term,
          products_array.subscription_created_at AS subscription_created_at,
          products_array.subscription_updated_at AS subscription_updated_at
        )
        ORDER BY
          products_array.subscription_updated_at DESC
      ) AS products
    )
  ) AS PAYLOAD
FROM
  `moz-fx-data-shared-prod.braze_derived.products_v2` AS products
CROSS JOIN
  UNNEST(products.products) AS products_array
WHERE
  products_array.subscription_updated_at > (SELECT max_update_timestamp FROM max_update)
GROUP BY
  products.external_id;
