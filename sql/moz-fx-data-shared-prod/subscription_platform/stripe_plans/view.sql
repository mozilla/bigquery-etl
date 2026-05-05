CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.stripe_plans`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.stripe_plans_v1`
