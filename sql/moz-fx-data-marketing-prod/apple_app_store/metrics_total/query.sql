SELECT
  * EXCEPT (
    active_devices_last_30_days,
    active_devices,
    crashes,
    deletions,
    installations,
    rate,
    sessions
  ),
  active_devices_last_30_days AS active_devices_last_30_days_opt_in,
  active_devices AS active_devices_opt_in,
  crashes AS crashes_opt_in,
  deletions AS deletions_opt_in,
  installations AS installations_opt_in,
  sessions AS sessions_opt_in,
  rate AS opt_in_rate
FROM
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_last_30_days_total`
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_total`
  USING (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.app_units_total`
  USING (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.crashes_total`
  USING (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.deletions_total`
  USING (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.iap_total`
  USING (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_total`
  USING (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_unique_device_total`
  USING (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.installations_total`
  USING (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.paying_users_total`
  USING (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_total`
  USING (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_unique_device_total`
  USING (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.rate_total`
  USING (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.sales_total`
  USING (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.sessions_total`
  USING (date, app_name)
WHERE
  date = @submission_date
