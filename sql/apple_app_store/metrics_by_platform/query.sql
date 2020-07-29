SELECT
  *
FROM
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_by_opt_in_platform`
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_last_30_days_by_opt_in_platform`
USING
  (date, app_name, platform)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.crashes_by_opt_in_platform`
USING
  (date, app_name, platform)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.deletions_by_opt_in_platform`
USING
  (date, app_name, platform)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.installations_by_opt_in_platform`
USING
  (date, app_name, platform)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.sessions_by_opt_in_platform`
USING
  (date, app_name, platform)
WHERE
  date = @date
