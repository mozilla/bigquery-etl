CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_fire_tv.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_fire_tv_derived.metrics_clients_last_seen_v1`
