CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.login_flows`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.login_flows_v1
