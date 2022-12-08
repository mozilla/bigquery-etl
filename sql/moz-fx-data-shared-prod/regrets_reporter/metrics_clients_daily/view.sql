CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.regrets_reporter.metrics_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.regrets_reporter_derived.metrics_clients_daily_v1`
