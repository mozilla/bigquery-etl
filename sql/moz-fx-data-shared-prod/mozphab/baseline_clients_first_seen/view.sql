-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `mozphab.baseline_clients_first_seen`
AS
SELECT
  *
FROM
  `mozphab_derived.baseline_clients_first_seen_v1`
