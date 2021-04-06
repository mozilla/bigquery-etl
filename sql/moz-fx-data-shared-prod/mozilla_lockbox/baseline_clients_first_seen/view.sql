-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `mozilla_lockbox.baseline_clients_first_seen`
AS
SELECT
  *
FROM
  `mozilla_lockbox_derived.baseline_clients_first_seen_v1`
