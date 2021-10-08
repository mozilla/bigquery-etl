CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.burnham.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.burnham_derived.metrics_clients_last_seen_v1`
