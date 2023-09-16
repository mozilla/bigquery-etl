CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.stripe_products`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.stripe_products_v1`
