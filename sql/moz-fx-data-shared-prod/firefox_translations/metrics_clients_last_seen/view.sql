CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_translations.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_translations_derived.metrics_clients_last_seen_v1`
