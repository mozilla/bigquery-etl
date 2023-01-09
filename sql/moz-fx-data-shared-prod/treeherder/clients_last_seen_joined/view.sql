CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.treeherder.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.treeherder_derived.clients_last_seen_joined_v1`
