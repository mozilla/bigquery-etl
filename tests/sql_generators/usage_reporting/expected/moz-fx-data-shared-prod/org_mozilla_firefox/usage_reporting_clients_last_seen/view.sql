-- Generated via `usage_reporting` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_firefox.usage_reporting_clients_last_seen`
AS
SELECT
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_derived.usage_reporting_clients_last_seen_v1`
