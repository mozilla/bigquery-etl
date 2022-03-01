CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.klar_android.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.klar_android_derived.metrics_clients_last_seen_v1`
