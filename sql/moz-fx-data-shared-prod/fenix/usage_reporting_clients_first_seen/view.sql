-- Generated via `usage_reporting` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.usage_reporting_clients_first_seen`
AS
SELECT
  "org_mozilla_firefox" AS normalized_app_id,
  "release" AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.usage_reporting_clients_first_seen`
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS normalized_app_id,
  "beta" AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.usage_reporting_clients_first_seen`
UNION ALL
SELECT
  "org_mozilla_fenix" AS normalized_app_id,
  "nightly" AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix.usage_reporting_clients_first_seen`
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS normalized_app_id,
  "nightly" AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.usage_reporting_clients_first_seen`
UNION ALL
SELECT
  "org_mozilla_fennec_aurora" AS normalized_app_id,
  "nightly" AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.usage_reporting_clients_first_seen`
