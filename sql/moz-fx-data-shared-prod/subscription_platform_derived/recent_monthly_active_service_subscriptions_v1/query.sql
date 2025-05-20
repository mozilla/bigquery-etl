SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.monthly_active_service_subscriptions_v1_live`
WHERE
  (month_start_date BETWEEN DATE_TRUNC((CURRENT_DATE() - 7), MONTH) AND (CURRENT_DATE() - 1))
