SELECT
  *
FROM
  `moz-fx-data-shared-prod`.hubs_derived.active_subscriptions_live
WHERE
  IF(CAST(@date AS DATE) IS NULL, active_date < CURRENT_DATE - 7, active_date = @date)
