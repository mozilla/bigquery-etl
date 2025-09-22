CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.daily_active_users_by_product_category`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.daily_active_users_by_product_category_v1`
