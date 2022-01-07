CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.subscription_events`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.subscription_events_v1
