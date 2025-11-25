-- Generated via `usage_reporting` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.usage_reporting_clients_last_seen`
AS
SELECT
  "firefox_desktop" AS normalized_app_id,
  `mozfun.norm.app_channel`(app_channel) AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.usage_reporting_clients_last_seen_v1`
