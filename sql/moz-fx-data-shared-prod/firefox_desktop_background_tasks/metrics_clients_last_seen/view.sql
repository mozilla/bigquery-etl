CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop_background_tasks.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_background_tasks_derived.metrics_clients_last_seen_v1`
