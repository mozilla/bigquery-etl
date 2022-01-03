CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.active_subscriptions`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.active_subscriptions_v1
