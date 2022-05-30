CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.klar_android.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.klar_android_derived.clients_last_seen_joined_v1`
