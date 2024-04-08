-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.events_stream`
AS
SELECT
  "org_mozilla_firefox" AS normalized_app_id,
  e.* REPLACE (
    mozfun.norm.fenix_app_info(
      "org_mozilla_firefox",
      client_info.app_build
    ).channel AS normalized_channel
  ),
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.events_stream` AS e
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS normalized_app_id,
  e.* REPLACE (
    mozfun.norm.fenix_app_info(
      "org_mozilla_firefox_beta",
      client_info.app_build
    ).channel AS normalized_channel
  ),
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.events_stream` AS e
UNION ALL
SELECT
  "org_mozilla_fenix" AS normalized_app_id,
  e.* REPLACE (
    mozfun.norm.fenix_app_info(
      "org_mozilla_fenix",
      client_info.app_build
    ).channel AS normalized_channel
  ),
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix.events_stream` AS e
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS normalized_app_id,
  e.* REPLACE (
    mozfun.norm.fenix_app_info(
      "org_mozilla_fenix_nightly",
      client_info.app_build
    ).channel AS normalized_channel
  ),
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.events_stream` AS e
UNION ALL
SELECT
  "org_mozilla_fennec_aurora" AS normalized_app_id,
  e.* REPLACE (
    mozfun.norm.fenix_app_info(
      "org_mozilla_fennec_aurora",
      client_info.app_build
    ).channel AS normalized_channel
  ),
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.events_stream` AS e
