-- Generated via `usage_reporting` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_ios.usage_reporting_clients_last_seen`
AS
SELECT
  "org_mozilla_ios_focus" AS normalized_app_id,
  "release" AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_focus.usage_reporting_clients_last_seen`
