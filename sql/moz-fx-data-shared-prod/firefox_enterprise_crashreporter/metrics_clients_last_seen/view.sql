CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_enterprise_crashreporter.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_enterprise_crashreporter_derived.metrics_clients_last_seen_v1`
