CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.retention_by_subscription`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.retention_by_subscription_v1
