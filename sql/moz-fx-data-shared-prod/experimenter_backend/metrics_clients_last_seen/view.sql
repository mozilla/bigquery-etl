CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.experimenter_backend.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.experimenter_backend_derived.metrics_clients_last_seen_v1`
