SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.subscription_events_live
WHERE
  IF(CAST(@date AS DATE) IS NULL, event_date < CURRENT_DATE - 7, event_date = @date)
