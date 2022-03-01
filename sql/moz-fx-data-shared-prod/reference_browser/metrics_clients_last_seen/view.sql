CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.reference_browser.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.reference_browser_derived.metrics_clients_last_seen_v1`
