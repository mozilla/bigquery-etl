CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozregression.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mozregression_derived.metrics_clients_last_seen_v1`
