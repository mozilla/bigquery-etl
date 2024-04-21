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
      plan_interval_count,
      event_timestamp AS update_timestamp
    )
    ORDER BY
      event_timestamp,
      plan_started_at,
      plan_ended_at,
      plan_id,
      product_id
  ) AS products
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.stripe_subscriptions_v1` AS subscriptions
LEFT JOIN
  `moz-fx-data-shared-prod.ctms_braze.ctms_fxa` AS fxa
  ON subscriptions.fxa_uid = TO_HEX(SHA256(fxa.fxa_id))
INNER JOIN
  `moz-fx-data-shared-prod.braze_derived.users_v1` AS users
  ON users.external_id = fxa.email_id
GROUP BY
  fxa_id_sha256;
