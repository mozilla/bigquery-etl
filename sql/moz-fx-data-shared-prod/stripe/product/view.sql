CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stripe.products`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.stripe_derived.products_v1
