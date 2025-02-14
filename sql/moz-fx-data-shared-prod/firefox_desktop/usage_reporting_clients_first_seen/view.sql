-- Generated via `usage_reporting` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.usage_reporting_clients_first_seen`
AS
SELECT
  "" AS normalized_app_id,
  `mozfun.norm.app_channel`(app_channel) AS normalized_channel,
  *,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.usage_reporting_clients_first_seen_v1`
