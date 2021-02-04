CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.subscriptions`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.subscriptions_v1
