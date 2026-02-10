-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.baseline_clients_first_seen`
AS
SELECT
  "org_mozilla_ios_firefox" AS normalized_app_id,
  * REPLACE ("release" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox.baseline_clients_first_seen`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxbeta" AS normalized_app_id,
  * REPLACE ("beta" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.baseline_clients_first_seen`
UNION ALL
SELECT
  "org_mozilla_ios_fennec" AS normalized_app_id,
  * REPLACE ("nightly" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_fennec.baseline_clients_first_seen`
