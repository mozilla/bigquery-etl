-- Generated via `usage_reporting` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_android.usage_reporting_clients_first_seen`
AS
SELECT
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus.usage_reporting_clients_first_seen`
UNION ALL
SELECT
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_beta.usage_reporting_clients_first_seen`
UNION ALL
SELECT
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_nightly.usage_reporting_clients_first_seen`
