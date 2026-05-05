CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring_derived.outerbounds_cost_per_flow_v1`
AS
WITH cost_per_flow AS (
  SELECT
    flow_name,
    SUM(cost_usd) AS total_cost_usd,
    COUNT(flow_name) AS num_runs
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.outerbounds_cost_per_flow_run_v1`
  GROUP BY
    flow_name
)
SELECT
  cost_per_flow.flow_name,
  cost_per_flow.total_cost_usd,
  cost_per_flow.num_runs,
  descriptions.flow_description,
FROM
  cost_per_flow
LEFT JOIN
  `moz-fx-data-shared-prod.monitoring_derived.outerbounds_flow_description_v1` AS descriptions
  ON LOWER(cost_per_flow.flow_name) LIKE CONCAT('%', LOWER(descriptions.flow_name), '%')
ORDER BY
  total_cost_usd DESC
