CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozillavpn_backend_cirrus.metrics_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mozillavpn_backend_cirrus_derived.metrics_clients_daily_v1`