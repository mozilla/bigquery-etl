SELECT
  event_date,
  event_type,
  granular_event_type,
  plan_id,
  status,
  country,
  country_name,
  entrypoint_experiment,
  entrypoint_variation,
  utm_campaign,
  utm_content,
  utm_medium,
  utm_source,
  utm_term,
  provider,
  plan_amount,
  billing_scheme,
  plan_currency,
  plan_interval,
  plan_interval_count,
  product_id,
  product_name,
  pricing_plan,
  normalized_acquisition_channel,
  normalized_campaign,
  normalized_content,
  normalized_medium,
  normalized_source,
  website_channel_group,
  COUNT(*) AS `count`,
FROM
  all_subscriptions_v1
CROSS JOIN
  UNNEST(
    ARRAY_CONCAT(
      [
        STRUCT(
          DATE(subscription_start_date) AS event_date,
          "New" AS event_type,
          IF(
            DATE(subscription_start_date) = DATE(customer_start_date),
            "New",
            "Resurrected"
          ) AS granular_event_type,
          1 AS delta
        )
      ],
      IF(
        DATE(end_date) < IFNULL(@date + 1, CURRENT_DATE),
        [
          STRUCT(
            DATE(end_date) AS event_date,
            "Cancelled" AS `type`,
            CASE
            WHEN
              provider LIKE "Apple Store IAP"
            THEN
              "Cancelled by IAP"
            WHEN
              canceled_for_customer_at IS NOT NULL
              OR cancel_at_period_end
            THEN
              "Cancelled by Customer"
            ELSE
              "Payment Failed"
            END
            AS granular_type,
            -1 AS delta
          )
        ],
        []
      )
    )
  )
WHERE
  @date IS NULL
  OR @date = event_date
GROUP BY
  event_date,
  event_type,
  granular_event_type,
  plan_id,
  status,
  country,
  country_name,
  entrypoint_experiment,
  entrypoint_variation,
  utm_campaign,
  utm_content,
  utm_medium,
  utm_source,
  utm_term,
  provider,
  plan_amount,
  billing_scheme,
  plan_currency,
  plan_interval,
  plan_interval_count,
  product_id,
  product_name,
  pricing_plan,
  normalized_acquisition_channel,
  normalized_campaign,
  normalized_content,
  normalized_medium,
  normalized_source,
  website_channel_group
