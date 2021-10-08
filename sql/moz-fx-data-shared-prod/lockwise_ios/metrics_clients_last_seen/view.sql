CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.lockwise_ios.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.lockwise_ios_derived.metrics_clients_last_seen_v1`
