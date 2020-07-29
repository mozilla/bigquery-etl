SELECT
  * EXCEPT (rate),
  rate AS opt_in_rate
FROM
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_by_opt_in_storefront`
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_last_30_days_by_opt_in_storefront`
USING
  (date, app_name, storefront)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.deletions_by_opt_in_storefront`
USING
  (date, app_name, storefront)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.installations_by_opt_in_storefront`
USING
  (date, app_name, storefront)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.rate_by_opt_in_storefront`
USING
  (date, app_name, storefront)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.sessions_by_opt_in_storefront`
USING
  (date, app_name, storefront)
WHERE
  date = @date
