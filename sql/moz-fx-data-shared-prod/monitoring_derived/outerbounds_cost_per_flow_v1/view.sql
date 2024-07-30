CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring_derived.outerbounds_cost_per_flow_v1`
AS
SELECT
  flow_name,
  SUM(cost_usd) AS total_cost_usd,
  COUNT(flow_name) AS num_runs
FROM
  `moz-fx-data-shared-prod.monitoring_derived.outerbounds_cost_per_flow_run_v1`
GROUP BY
  flow_name
ORDER BY
  total_cost_usd DESC
