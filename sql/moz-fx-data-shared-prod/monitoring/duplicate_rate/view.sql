CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.duplicate_rate`
AS
SELECT
  *,
  SAFE_DIVIDE(n_distinct, n) AS ratio
FROM
  `moz-fx-data-shared-prod.monitoring_derived.duplicate_rate_v1`
