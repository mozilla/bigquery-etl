-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.baseline_clients_first_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_derived.baseline_clients_daily_v1`
WHERE
  is_new_profile
