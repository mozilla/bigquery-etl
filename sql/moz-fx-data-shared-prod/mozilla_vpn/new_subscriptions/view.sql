CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.new_subscriptions`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.new_subscriptions_v1
