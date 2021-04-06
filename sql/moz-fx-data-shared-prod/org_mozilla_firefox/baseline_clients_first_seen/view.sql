-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `org_mozilla_firefox.baseline_clients_first_seen`
AS
SELECT
  *
FROM
  `org_mozilla_firefox_derived.baseline_clients_first_seen_v1`
