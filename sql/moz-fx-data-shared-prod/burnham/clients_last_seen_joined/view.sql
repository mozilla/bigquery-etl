CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.burnham.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.burnham_derived.clients_last_seen_joined_v1`
