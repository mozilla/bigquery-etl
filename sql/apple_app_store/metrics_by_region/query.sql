SELECT
  * EXCEPT (rate),
  rate AS opt_in_rate
FROM
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_by_opt_in_region`
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_last_30_days_by_opt_in_region`
USING
  (date, app_name, region)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.deletions_by_opt_in_region`
USING
  (date, app_name, region)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.installations_by_opt_in_region`
USING
  (date, app_name, region)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.rate_by_opt_in_region`
USING
  (date, app_name, region)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.sessions_by_opt_in_region`
USING
  (date, app_name, region)
WHERE
  date = @date
