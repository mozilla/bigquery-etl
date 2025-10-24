CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.experimenter_backend.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.experimenter_backend_derived.clients_last_seen_joined_v1`
