-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.lockwise_android.baseline_clients_last_seen`
AS
SELECT
  "mozilla_lockbox" AS normalized_app_id,
  * REPLACE ("release" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.mozilla_lockbox.baseline_clients_last_seen`
