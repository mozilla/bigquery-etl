CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.bergamot.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.bergamot_derived.clients_last_seen_joined_v1`
