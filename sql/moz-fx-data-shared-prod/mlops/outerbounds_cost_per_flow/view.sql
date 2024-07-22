CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mlops.outerbounds_cost_per_flow`
AS
WITH cost_by_run as (
SELECT
  DATE_TRUNC(usage_start_time, DAY) as invoice_day,
  labels.value AS run_id,
  labels.value,
  ifnull(safe.left(labels.value, instr(labels.value, '-', -1) - 1), labels.value) as flow_name,
  SUM(cost) as cost_usd
FROM
  moz-fx-data-shared-prod.billing_syndicate.gcp_billing_export_resource_v1_01E7D5_97288E_E2EBA0
JOIN
  UNNEST(labels) AS labels
ON
  labels.key = "k8s-label/workflows.argoproj.io/workflow"
WHERE
  project.id = "moz-fx-mfouterbounds-prod-f98d"
  AND usage_start_time >= '2024-07-10'
GROUP BY
  1,
  2
ORDER BY 3 DESC
)
SELECT
  flow_name,
  sum(cost_usd) as total_cost_usd,
  count(flow_name) as num_runs
FROM cost_by_run
GROUP BY 1
ORDER BY 2 DESC