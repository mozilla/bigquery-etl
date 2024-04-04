SELECT
  fxa_uid AS fxa_id_sha256,
  ARRAY_AGG(
    STRUCT(
      plan_id,
      product_id,
      status,
      plan_started_at,
      plan_ended_at,
      plan_interval,
      plan_interval_count
    )
  ) AS products
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.stripe_subscriptions_v1`
GROUP BY
  fxa_id_sha256;
