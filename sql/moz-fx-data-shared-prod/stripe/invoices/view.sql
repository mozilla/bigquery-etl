CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stripe.invoices`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.stripe_derived.invoices_v1
