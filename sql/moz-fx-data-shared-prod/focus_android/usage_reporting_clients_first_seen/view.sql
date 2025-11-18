-- Generated via `usage_reporting` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_android.usage_reporting_clients_first_seen`
AS
SELECT
  "org_mozilla_focus" AS normalized_app_id,
  "release" AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus.usage_reporting_clients_first_seen`
UNION ALL
SELECT
  "org_mozilla_focus_beta" AS normalized_app_id,
  "beta" AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_beta.usage_reporting_clients_first_seen`
UNION ALL
SELECT
  "org_mozilla_focus_nightly" AS normalized_app_id,
  "nightly" AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_nightly.usage_reporting_clients_first_seen`
