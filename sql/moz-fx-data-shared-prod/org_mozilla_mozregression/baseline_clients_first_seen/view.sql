-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `org_mozilla_mozregression.baseline_clients_first_seen`
AS
SELECT
  *
FROM
  `org_mozilla_mozregression_derived.baseline_clients_first_seen_v1`
