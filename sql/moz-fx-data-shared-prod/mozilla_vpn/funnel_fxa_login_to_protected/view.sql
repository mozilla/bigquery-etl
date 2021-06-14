CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.funnel_fxa_login_to_protected`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.funnel_fxa_login_to_protected_v1
