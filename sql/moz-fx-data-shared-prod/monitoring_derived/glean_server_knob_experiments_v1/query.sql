CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring_derived.glean_server_knob_experiments`
AS
SELECT
  normandy_slug AS slug,
  app_name,
  app_id,
  start_date,
  end_date,
  status,
  targeted_percent,
  targeting,
  feature.value.gleanMetricConfiguration AS glean_metric_config
FROM
  `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
CROSS JOIN
  UNNEST(branches) AS branch
CROSS JOIN
  UNNEST(JSON_EXTRACT_ARRAY(branch.features)) AS feature
WHERE
  "glean" IN UNNEST(feature_ids)
  AND STRING(feature.featureId) = "glean"
  AND JSON_QUERY(feature.value, "$.gleanMetricConfiguration") IS NOT NULL
