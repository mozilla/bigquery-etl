SELECT
  * EXCEPT (
    active_devices,
    active_devices_last_30_days,
    crashes,
    deletions,
    installations,
    sessions
  ),
  active_devices AS active_devices_opt_in,
  active_devices_last_30_days AS active_devices_last_30_days_opt_in,
  crashes AS crashes_opt_in,
  deletions AS deletions_opt_in,
  installations AS installations_opt_in,
  sessions AS sessions_opt_in
FROM
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_by_opt_in_platform`
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_last_30_days_by_opt_in_platform`
USING
  (date, app_name, platform)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.app_units_by_platform`
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
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_by_platform`
USING
  (date, app_name, platform)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_unique_device_by_platform`
USING
  (date, app_name, platform)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.installations_by_opt_in_platform`
USING
  (date, app_name, platform)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_by_platform`
USING
  (date, app_name, platform)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_unique_device_by_platform`
USING
  (date, app_name, platform)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.sessions_by_opt_in_platform`
USING
  (date, app_name, platform)
WHERE
  date = @date
