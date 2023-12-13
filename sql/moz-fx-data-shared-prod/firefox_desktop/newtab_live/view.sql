CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.newtab_live`
AS
SELECT
  submission_timestamp,
  normalized_country_code,
  normalized_channel,
  document_id,
  metrics.boolean.pocket_enabled,
  metrics.boolean.pocket_sponsored_stories_enabled,
  metrics.string.newtab_locale,
  client_info.app_display_version,
  client_info.client_id,
  events
FROM
  `moz-fx-data-shared-prod.firefox_desktop_live.newtab_v1`
