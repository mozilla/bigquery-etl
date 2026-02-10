CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozphab.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mozphab_derived.metrics_clients_last_seen_v1`
