-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.tiktokreporter_android.baseline_clients_last_seen`
AS
SELECT
  "org_mozilla_tiktokreporter" AS normalized_app_id,
  * REPLACE ("release" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_tiktokreporter.baseline_clients_last_seen`
