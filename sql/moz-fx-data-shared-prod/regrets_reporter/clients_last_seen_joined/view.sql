CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.regrets_reporter.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.regrets_reporter_derived.clients_last_seen_joined_v1`
