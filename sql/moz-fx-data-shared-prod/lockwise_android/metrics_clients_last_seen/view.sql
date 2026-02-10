CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.lockwise_android.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.lockwise_android_derived.metrics_clients_last_seen_v1`
