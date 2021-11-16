CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.regrets_reporter.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.regrets_reporter_derived.metrics_clients_last_seen_v1`
