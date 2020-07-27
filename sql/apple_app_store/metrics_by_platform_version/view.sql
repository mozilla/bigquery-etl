CREATE OR REPLACE VIEW
  `moz-fx-data-marketing-prod.apple_app_store.metrics_by_platform_version`
AS
SELECT
  *
FROM
  `moz-fx-data-marketing-prod.apple_app_store_exported.app_units_by_platform_version`
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_by_platform_version`
USING
  (date, app_name, platform_version)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_unique_device_by_platform_version`
USING
  (date, app_name, platform_version)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_by_platform_version`
USING
  (date, app_name, platform_version)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_unique_device_by_platform_version`
USING
  (date, app_name, platform_version)
ORDER BY
  date,
  app_name,
  platform_version
