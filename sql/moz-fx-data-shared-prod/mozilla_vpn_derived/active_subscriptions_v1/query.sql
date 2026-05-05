SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.active_subscriptions_live
WHERE
  IF(CAST(@date AS DATE) IS NULL, active_date < CURRENT_DATE - 7, active_date = @date)
