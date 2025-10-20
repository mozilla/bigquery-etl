-- Generated via `usage_reporting` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.usage_reporting_clients_last_seen`
AS
SELECT
  "org_mozilla_firefox" AS normalized_app_id,
  "release" AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.usage_reporting_clients_last_seen`
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS normalized_app_id,
  "beta" AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.usage_reporting_clients_last_seen`
