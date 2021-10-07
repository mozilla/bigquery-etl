CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozphab.metrics_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mozphab_derived.metrics_clients_daily_v1`
