-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_echo_show.baseline_clients_daily`
AS
SELECT
  "org_mozilla_connect_firefox" AS normalized_app_id,
  * REPLACE ("release" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_connect_firefox.baseline_clients_daily`
