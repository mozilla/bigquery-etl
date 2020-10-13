CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.waitlist`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.waitlist_v1
