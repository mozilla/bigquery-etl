-- Generated via `usage_reporting` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.usage_reporting_clients_daily`
AS
SELECT
  "org_mozilla_ios_firefox" AS normalized_app_id,
  "release" AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox.usage_reporting_clients_daily`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxbeta" AS normalized_app_id,
  "beta" AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.usage_reporting_clients_daily`
UNION ALL
SELECT
  "org_mozilla_ios_fennec" AS normalized_app_id,
  "nightly" AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_fennec.usage_reporting_clients_daily`
