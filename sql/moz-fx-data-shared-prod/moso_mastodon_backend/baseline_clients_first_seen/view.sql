-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.moso_mastodon_backend.baseline_clients_first_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.moso_mastodon_backend_derived.baseline_clients_daily_v1`
WHERE
  is_new_profile
