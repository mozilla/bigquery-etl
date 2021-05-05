CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stripe.subscriptions`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.stripe_derived.subscriptions_v1
