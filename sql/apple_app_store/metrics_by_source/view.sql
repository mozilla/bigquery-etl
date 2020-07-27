CREATE OR REPLACE VIEW
  `moz-fx-data-marketing-prod.apple_app_store.metrics_by_source`
AS
SELECT
  *
FROM
  `moz-fx-data-marketing-prod.apple_app_store_exported.app_units_by_source`
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_by_source`
USING
  (date, app_name, source)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_unique_device_by_source`
USING
  (date, app_name, source)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_by_source`
USING
  (date, app_name, source)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_unique_device_by_source`
USING
  (date, app_name, source)
ORDER BY
  date,
  app_name,
  source
