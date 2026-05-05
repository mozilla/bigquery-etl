CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fxci.worker_metrics`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.fxci_derived.worker_metrics_v1`
