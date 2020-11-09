CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stripe.customers`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.stripe_derived.customers_v1
