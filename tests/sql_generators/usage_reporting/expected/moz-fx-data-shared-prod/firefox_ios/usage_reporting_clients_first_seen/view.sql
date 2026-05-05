-- Generated via `usage_reporting` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.usage_reporting_clients_first_seen`
AS
SELECT
  "org_mozilla_ios_firefoxbeta" AS normalized_app_id,
  "beta" AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.usage_reporting_clients_first_seen`
