CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.clients_first_seen_28_days_later`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.clients_first_seen_28_days_later_v1`
