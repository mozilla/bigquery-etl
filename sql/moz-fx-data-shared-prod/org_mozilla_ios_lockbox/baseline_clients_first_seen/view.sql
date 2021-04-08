-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `org_mozilla_ios_lockbox.baseline_clients_first_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_lockbox_derived.baseline_clients_first_seen_v1`
