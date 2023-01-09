CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.treeherder.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.treeherder_derived.metrics_clients_last_seen_v1`
