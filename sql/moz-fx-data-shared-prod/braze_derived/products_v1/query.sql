SELECT
  fxa.email_id AS external_id,
  ARRAY_AGG(
    STRUCT(
      subscriptions.plan_id AS plan_id,
      subscriptions.product_id AS product_id,
      subscriptions.status AS status,
      subscriptions.plan_started_at AS plan_started_at,
      subscriptions.plan_ended_at AS plan_ended_at,
      subscriptions.plan_interval AS plan_interval,
      subscriptions.plan_interval_count AS plan_interval_count,
      subscriptions.event_timestamp AS update_timestamp
    )
    ORDER BY
      subscriptions.event_timestamp,
      subscriptions.plan_started_at,
      subscriptions.plan_ended_at,
      subscriptions.plan_id,
      subscriptions.product_id
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
  email_id;
