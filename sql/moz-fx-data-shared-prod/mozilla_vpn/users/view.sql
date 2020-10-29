CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.users`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.users_v1
