CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop_background_update.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_background_update_derived.metrics_clients_last_seen_v1`
