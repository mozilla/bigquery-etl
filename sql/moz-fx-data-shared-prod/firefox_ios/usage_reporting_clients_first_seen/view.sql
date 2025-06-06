-- Generated via `usage_reporting` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.usage_reporting_clients_first_seen`
AS
SELECT
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox.usage_reporting_clients_first_seen`
UNION ALL
SELECT
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.usage_reporting_clients_first_seen`
UNION ALL
SELECT
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_fennec.usage_reporting_clients_first_seen`
