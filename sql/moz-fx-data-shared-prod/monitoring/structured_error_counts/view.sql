CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.structured_error_counts`
AS
SELECT
  *,
  CAST(
    submission_date AS TIMESTAMP
  ) AS hour -- for backwards compatibility
FROM
  `moz-fx-data-shared-prod.monitoring_derived.structured_error_counts_v2`
