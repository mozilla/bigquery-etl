CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.clients_last_seen_joined_v1`
