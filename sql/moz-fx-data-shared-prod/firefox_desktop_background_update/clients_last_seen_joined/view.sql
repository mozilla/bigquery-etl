CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop_background_update.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_background_update_derived.clients_last_seen_joined_v1`
