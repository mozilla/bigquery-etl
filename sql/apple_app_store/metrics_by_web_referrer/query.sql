SELECT
  *
FROM
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_by_opt_in_web_referrer`
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_last_30_days_by_opt_in_web_referrer`
USING
  (date, app_name, web_referrer)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.deletions_by_opt_in_web_referrer`
USING
  (date, app_name, web_referrer)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.installations_by_opt_in_web_referrer`
USING
  (date, app_name, web_referrer)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.sessions_by_opt_in_web_referrer`
USING
  (date, app_name, web_referrer)
WHERE
  date = @date
