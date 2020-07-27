CREATE OR REPLACE VIEW
  `moz-fx-data-marketing-prod.apple_app_store.metrics_by_region`
AS
SELECT
  *
FROM
  `moz-fx-data-marketing-prod.apple_app_store_exported.app_units_by_region`
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_by_region`
USING
  (date, app_name, region)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_unique_device_by_region`
USING
  (date, app_name, region)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_by_region`
USING
  (date, app_name, region)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_unique_device_by_region`
USING
  (date, app_name, region)
ORDER BY
  date,
  app_name,
  region
