-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.baseline_clients_first_seen`
AS
SELECT
  * REPLACE (
    mozfun.norm.fenix_app_info("org_mozilla_firefox", app_build).channel AS normalized_channel
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.baseline_clients_first_seen`
UNION ALL
SELECT
  * REPLACE (
    mozfun.norm.fenix_app_info("org_mozilla_firefox_beta", app_build).channel AS normalized_channel
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.baseline_clients_first_seen`
UNION ALL
SELECT
  * REPLACE (
    mozfun.norm.fenix_app_info("org_mozilla_fenix", app_build).channel AS normalized_channel
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix.baseline_clients_first_seen`
UNION ALL
SELECT
  * REPLACE (
    mozfun.norm.fenix_app_info("org_mozilla_fenix_nightly", app_build).channel AS normalized_channel
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.baseline_clients_first_seen`
UNION ALL
SELECT
  * REPLACE (
    mozfun.norm.fenix_app_info("org_mozilla_fennec_aurora", app_build).channel AS normalized_channel
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.baseline_clients_first_seen`
