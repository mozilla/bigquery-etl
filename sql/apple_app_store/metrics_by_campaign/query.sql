SELECT
  * EXCEPT (active_devices, active_devices_last_30_days, deletions, installations, sessions),
  active_devices AS active_devices_opt_in,
  active_devices_last_30_days AS active_devices_last_30_days_opt_in,
  deletions AS deletions_opt_in,
  installations AS installations_opt_in,
  sessions AS sessions_opt_in
FROM
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_by_opt_in_campaign`
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_last_30_days_by_opt_in_campaign`
USING
  (date, app_name, campaign)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.app_units_by_campaign`
USING
  (date, app_name, campaign)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.deletions_by_opt_in_campaign`
USING
  (date, app_name, campaign)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_by_campaign`
USING
  (date, app_name, campaign)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_unique_device_by_campaign`
USING
  (date, app_name, campaign)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.installations_by_opt_in_campaign`
USING
  (date, app_name, campaign)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_by_campaign`
USING
  (date, app_name, campaign)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_unique_device_by_campaign`
USING
  (date, app_name, campaign)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.sessions_by_opt_in_campaign`
USING
  (date, app_name, campaign)
WHERE
  date = @date
