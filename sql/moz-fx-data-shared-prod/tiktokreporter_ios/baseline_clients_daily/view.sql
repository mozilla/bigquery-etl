-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.tiktokreporter_ios.baseline_clients_daily`
AS
SELECT
  "org_mozilla_ios_tiktok_reporter" AS normalized_app_id,
  * REPLACE ("release" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_tiktok_reporter.baseline_clients_daily`
