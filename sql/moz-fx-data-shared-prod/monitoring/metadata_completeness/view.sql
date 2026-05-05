CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.metadata_completeness`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.metadata_completeness_v1`
