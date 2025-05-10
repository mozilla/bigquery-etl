SELECT
  subscription.*
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.google_subscriptions_history_v1`
WHERE
  valid_to = '9999-12-31 23:59:59.999999'
