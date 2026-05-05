CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.looker_usage_unused_explores`
AS
SELECT
  Model AS model,
  Explore AS explore,
  `Unused Joins` AS unused_joins,
  `Unused Fields` AS unused_fields,
  submission_date
FROM
  `moz-fx-data-shared-prod.monitoring_derived.looker_usage_unused_explores_v1`
