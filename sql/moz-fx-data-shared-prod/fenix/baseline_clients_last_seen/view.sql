-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.baseline_clients_last_seen`
AS
SELECT
  "org_mozilla_firefox" AS normalized_app_id,
  * REPLACE (
    mozfun.norm.fenix_app_info("org_mozilla_firefox", app_build).channel AS normalized_channel
  ),
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.baseline_clients_last_seen`
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS normalized_app_id,
  * REPLACE (
    mozfun.norm.fenix_app_info("org_mozilla_firefox_beta", app_build).channel AS normalized_channel
  ),
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.baseline_clients_last_seen`
UNION ALL
SELECT
  "org_mozilla_fenix" AS normalized_app_id,
  * REPLACE (
    mozfun.norm.fenix_app_info("org_mozilla_fenix", app_build).channel AS normalized_channel
  ),
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix.baseline_clients_last_seen`
