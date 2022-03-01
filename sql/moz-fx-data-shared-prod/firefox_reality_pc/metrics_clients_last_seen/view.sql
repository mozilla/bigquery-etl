CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_reality_pc.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_reality_pc_derived.metrics_clients_last_seen_v1`
