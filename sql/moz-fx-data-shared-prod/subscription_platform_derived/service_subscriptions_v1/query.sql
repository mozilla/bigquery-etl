SELECT
  subscription.id,
  subscription.service.id AS service_id,
  subscription.* EXCEPT (id)
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.service_subscriptions_history_v1`
WHERE
  valid_to = '9999-12-31 23:59:59.999999'
