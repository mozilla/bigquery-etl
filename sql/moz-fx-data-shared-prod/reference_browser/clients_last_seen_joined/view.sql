CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.reference_browser.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.reference_browser_derived.clients_last_seen_joined_v1`
