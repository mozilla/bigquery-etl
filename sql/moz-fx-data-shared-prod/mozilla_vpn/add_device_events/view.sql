CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.add_device_events`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.add_device_events_v1
