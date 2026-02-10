-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mach.baseline_clients_first_seen`
AS
SELECT
  "mozilla_mach" AS normalized_app_id,
  * REPLACE ("release" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.mozilla_mach.baseline_clients_first_seen`
