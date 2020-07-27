CREATE OR REPLACE VIEW
  `moz-fx-data-marketing-prod.apple_app_store.metrics_total`
AS
SELECT
  * EXCEPT (rate),
  rate AS opt_in_rate
FROM
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_last_30_days_total`
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.active_devices_total`
USING
  (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.app_units_total`
USING
  (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.crashes_total`
USING
  (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.deletions_total`
USING
  (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_total`
USING
  (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_unique_device_total`
USING
  (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.installations_total`
USING
  (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_total`
USING
  (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_unique_device_total`
USING
  (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.rate_total`
USING
  (date, app_name)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.sessions_total`
USING
  (date, app_name)
ORDER BY
  date,
  app_name
