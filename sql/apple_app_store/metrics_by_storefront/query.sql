SELECT
  * EXCEPT (active_devices, active_devices_last_30_days, deletions, installations, rate, sessions),
  active_devices AS active_devices_opt_in,
  active_devices_last_30_days AS active_devices_last_30_days_opt_in,
  deletions AS deletions_opt_in,
  installations AS installations_opt_in,
  sessions AS sessions_opt_in,
  rate AS opt_in_rate
FROM
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_by_opt_in_storefront`
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_last_30_days_by_opt_in_storefront`
USING
  (date, app_name, storefront)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.app_units_by_storefront`
USING
  (date, app_name, storefront)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.deletions_by_opt_in_storefront`
USING
  (date, app_name, storefront)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_by_storefront`
USING
  (date, app_name, storefront)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_unique_device_by_storefront`
USING
  (date, app_name, storefront)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.installations_by_opt_in_storefront`
USING
  (date, app_name, storefront)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_by_storefront`
USING
  (date, app_name, storefront)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_unique_device_by_storefront`
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
