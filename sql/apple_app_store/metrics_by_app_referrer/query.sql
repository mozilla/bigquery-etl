SELECT
  *
FROM
  `moz-fx-data-marketing-prod.apple_app_store_exported.app_units_by_app_referrer`
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_by_app_referrer`
USING
  (date, app_name, app_referrer)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.impressions_unique_device_by_app_referrer`
USING
  (date, app_name, app_referrer)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_by_app_referrer`
USING
  (date, app_name, app_referrer)
FULL JOIN
  `moz-fx-data-marketing-prod.apple_app_store_exported.product_page_views_unique_device_by_app_referrer`
USING
  (date, app_name, app_referrer)
WHERE
  date = @date
