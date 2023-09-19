SELECT
  *
FROM
  `moz-fx-data-shared-prod`.hubs_derived.subscription_events_live
WHERE
  IF(CAST(@date AS DATE) IS NULL, event_date < CURRENT_DATE - 8, event_date = @date)
