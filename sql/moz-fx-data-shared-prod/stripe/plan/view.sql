CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stripe.plans`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.stripe_derived.plans_v1
