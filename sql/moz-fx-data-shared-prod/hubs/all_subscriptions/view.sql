CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.all_subscriptions`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.all_subscriptions_v1
