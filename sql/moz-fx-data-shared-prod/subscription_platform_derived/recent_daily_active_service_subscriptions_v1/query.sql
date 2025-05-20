SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.daily_active_service_subscriptions_v1_live`
WHERE
  (`date` BETWEEN (CURRENT_DATE() - 7) AND (CURRENT_DATE() - 1))
