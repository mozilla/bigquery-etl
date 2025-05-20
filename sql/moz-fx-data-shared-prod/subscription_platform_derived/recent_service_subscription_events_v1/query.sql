SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.service_subscription_events_v1_live`
WHERE
  (DATE(`timestamp`) BETWEEN (CURRENT_DATE() - 7) AND (CURRENT_DATE() - 1))
