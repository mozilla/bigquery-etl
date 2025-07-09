CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.looker_usage_explores`
AS
SELECT
  Model AS model,
  Explore AS explore,
  `Is Hidden` AS is_hidden,
  `Has Description` AS has_description,
  `# Joins` AS total_joins,
  `# Unused Joins` AS total_unused_joins,
  `# Fields` AS total_fields,
  `# Unused Fields` AS total_unused_fields,
  `Query Count` AS query_count,
  submission_date
FROM
  `moz-fx-data-shared-prod.monitoring_derived.looker_usage_explores_v1`
