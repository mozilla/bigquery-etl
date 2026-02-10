CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_crashreporter.metrics_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_crashreporter_derived.metrics_clients_daily_v1`
