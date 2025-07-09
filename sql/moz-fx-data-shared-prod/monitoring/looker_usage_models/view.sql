CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.looker_usage_models`
AS
SELECT
  Project AS project,
  Model AS model,
  `# Explores` AS total_explores,
  `# Unused Explores` AS total_unused_explores,
  `Query Count` AS query_count,
  submission_date
FROM
  `moz-fx-data-shared-prod.monitoring_derived.looker_usage_models_v1`
