-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `burnham.baseline_clients_first_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.burnham_derived.baseline_clients_first_seen_v1`
