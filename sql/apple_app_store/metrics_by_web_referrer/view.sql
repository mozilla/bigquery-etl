CREATE OR REPLACE VIEW
  `moz-fx-data-marketing-prod.apple_app_store.metrics_by_web_referrer`
AS
SELECT
  *
FROM
  `moz-fx-data-marketing-prod.apple_app_store_exported.app_units_by_web_referrer`
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_by_web_referrer`
USING
  (date, app_name, web_referrer)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_unique_device_by_web_referrer`
USING
  (date, app_name, web_referrer)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_by_web_referrer`
USING
  (date, app_name, web_referrer)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_unique_device_by_web_referrer`
USING
  (date, app_name, web_referrer)
ORDER BY
  date,
  app_name,
  web_referrer
