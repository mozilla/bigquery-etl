-- Retrieves the maximum subscription updated timestamp from the last run to only
-- select recently changed records
WITH max_update AS (
  SELECT
    MAX(
      TIMESTAMP(JSON_VALUE(payload.products_v1[0].subscription_updated_at, '$."$time"'))
    ) AS latest_subscription_updated_at
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_products_sync_v1`
),
-- Counts the number of subscriptions per product
products_with_counts AS (
  SELECT
    products.external_id,
    products_array.*,
    COUNT(*) OVER (
      PARTITION BY
        products.external_id,
        products_array.product_name
    ) AS subscription_count
  FROM
    `moz-fx-data-shared-prod.braze_derived.products_v1` AS products
  CROSS JOIN
    UNNEST(products.products) AS products_array
  WHERE
    products_array.subscription_updated_at > (SELECT latest_subscription_updated_at FROM max_update)
)
-- Construct the JSON payload in Braze required format
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  products.external_id AS EXTERNAL_ID,
  TO_JSON(
    STRUCT(
      ARRAY_AGG(
        STRUCT(
          products.provider AS provider,
          products.payment_provider AS payment_provider,
          products.product_name AS product_name,
          products.service_name AS service_name,
          products.subscription_plan_id AS subscription_plan_id,
          products.subscription_product_id AS subscription_product_id,
          (
            CASE
              WHEN products.subscription_started_at IS NOT NULL -- null timestamp values cause errors in Braze
                THEN STRUCT(
                    FORMAT_TIMESTAMP(
                      '%Y-%m-%d %H:%M:%E6S UTC', -- format is Braze requirement for nested timestamps
                      products.subscription_started_at,
                      'UTC'
                    ) AS `$time`
                  )
            END
          ) AS subscription_started_at,
          (
            CASE
              WHEN products.subscription_ended_at IS NOT NULL
                THEN STRUCT(
                    FORMAT_TIMESTAMP(
                      '%Y-%m-%d %H:%M:%E6S UTC',
                      products.subscription_ended_at,
                      'UTC'
                    ) AS `$time`
                  )
            END
          ) AS subscription_ended_at,
          products.subscription_summary AS subscription_summary,
          products.subscription_billing_interval AS subscription_billing_interval,
          products.subscription_interval_count AS subscription_interval_count,
          products.subscription_currency AS subscription_currency,
          products.subscription_amount AS subscription_amount,
          products.is_bundle AS is_bundle,
          products.is_trial AS is_trial,
          products.is_active AS is_active,
          products.subscription_status AS subscription_status,
          products.subscription_count AS subscription_count,
          products.subscription_country_code AS subscription_country_code,
          products.subscription_country_name AS subscription_country_name,
          (
            CASE
              WHEN products.current_period_started_at IS NOT NULL
                THEN STRUCT(
                    FORMAT_TIMESTAMP(
                      '%Y-%m-%d %H:%M:%E6S UTC',
                      products.current_period_started_at,
                      'UTC'
                    ) AS `$time`
                  )
            END
          ) AS current_period_started_at,
          (
            CASE
              WHEN products.current_period_ends_at IS NOT NULL
                THEN STRUCT(
                    FORMAT_TIMESTAMP(
                      '%Y-%m-%d %H:%M:%E6S UTC',
                      products.current_period_ends_at,
                      'UTC'
                    ) AS `$time`
                  )
            END
          ) AS current_period_ends_at,
          products.is_auto_renew AS is_auto_renew,
          (
            CASE
              WHEN products.auto_renew_disabled_at IS NOT NULL
                THEN STRUCT(
                    FORMAT_TIMESTAMP(
                      '%Y-%m-%d %H:%M:%E6S UTC',
                      products.auto_renew_disabled_at,
                      'UTC'
                    ) AS `$time`
                  )
            END
          ) AS auto_renew_disabled_at,
          products.has_refunds AS has_refunds,
          products.has_fraudulent_charges AS has_fraudulent_charges,
          (
            CASE
              WHEN products.first_touch_impression_at IS NOT NULL
                THEN STRUCT(
                    FORMAT_TIMESTAMP(
                      '%Y-%m-%d %H:%M:%E6S UTC',
                      products.first_touch_impression_at,
                      'UTC'
                    ) AS `$time`
                  )
            END
          ) AS first_touch_impression_at,
          products.first_touch_entrypoint AS first_touch_entrypoint,
          products.first_touch_entrypoint_experiment AS first_touch_entrypoint_experiment,
          products.first_touch_entrypoint_variation AS first_touch_entrypoint_variation,
          products.first_touch_utm_campaign AS first_touch_utm_campaign,
          products.first_touch_utm_content AS first_touch_utm_content,
          products.first_touch_utm_medium AS first_touch_utm_medium,
          products.first_touch_utm_source AS first_touch_utm_source,
          products.first_touch_utm_term AS first_touch_utm_term,
          (
            CASE
              WHEN products.last_touch_impression_at IS NOT NULL
                THEN STRUCT(
                    FORMAT_TIMESTAMP(
                      '%Y-%m-%d %H:%M:%E6S UTC',
                      products.last_touch_impression_at,
                      'UTC'
                    ) AS `$time`
                  )
            END
          ) AS last_touch_impression_at,
          products.last_touch_entrypoint AS last_touch_entrypoint,
          products.last_touch_entrypoint_experiment AS last_touch_entrypoint_experiment,
          products.last_touch_entrypoint_variation AS last_touch_entrypoint_variation,
          products.last_touch_utm_campaign AS last_touch_utm_campaign,
          products.last_touch_utm_content AS last_touch_utm_content,
          products.last_touch_utm_medium AS last_touch_utm_medium,
          products.last_touch_utm_source AS last_touch_utm_source,
          products.last_touch_utm_term AS last_touch_utm_term,
          STRUCT(
            FORMAT_TIMESTAMP(
              '%Y-%m-%d %H:%M:%E6S UTC',
              products.subscription_created_at,
              'UTC'
            ) AS `$time`
          ) AS subscription_created_at,
          STRUCT(
            FORMAT_TIMESTAMP(
              '%Y-%m-%d %H:%M:%E6S UTC',
              products.subscription_updated_at,
              'UTC'
            ) AS `$time`
          ) AS subscription_updated_at
        )
        ORDER BY
          products.subscription_updated_at DESC
      ) AS products_v1
    )
  ) AS PAYLOAD
FROM
  products_with_counts AS products
GROUP BY
  products.external_id;
