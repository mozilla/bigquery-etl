CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.metrics_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mozilla_vpn_derived.metrics_clients_daily_v1`
