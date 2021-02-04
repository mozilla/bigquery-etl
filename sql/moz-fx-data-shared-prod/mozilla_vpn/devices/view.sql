CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.devices`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.devices_v1
