-- Generated via ./bqetl glean_usage generate
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.regrets_reporter.baseline_clients_daily`
AS
SELECT
  * REPLACE ("release" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.regrets_reporter_ucs.baseline_clients_daily`
