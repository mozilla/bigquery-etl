-- Generated via ./bqetl glean_usage generate
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.baseline_clients_first_seen`
AS
SELECT
  * REPLACE ("release" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.baseline_clients_first_seen`
UNION ALL
SELECT
  * REPLACE ("beta" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.baseline_clients_first_seen`
UNION ALL
SELECT
  * REPLACE ("nightly" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix.baseline_clients_first_seen`
UNION ALL
SELECT
  * REPLACE ("nightly" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.baseline_clients_first_seen`
UNION ALL
SELECT
  * REPLACE ("nightly" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.baseline_clients_first_seen`
