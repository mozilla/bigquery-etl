CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glean_dictionary.metrics_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.glean_dictionary_derived.metrics_clients_daily_v1`
