CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stripe.itemized_tax_transactions`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.stripe_external.itemized_tax_transactions_v1
