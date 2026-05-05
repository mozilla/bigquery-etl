CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring_derived.outerbounds_cost_per_flow_run_v1`
AS
SELECT
  DATE_TRUNC(usage_start_time, DAY) AS invoice_day,
  labels.value AS run_id,
  IFNULL(SAFE.LEFT(labels.value, INSTR(labels.value, '-', -1) - 1), labels.value) AS flow_name,
  SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) AS cost_usd
FROM
  `moz-fx-data-shared-prod.billing_syndicate.gcp_billing_export_resource_v1_01E7D5_97288E_E2EBA0`
JOIN
  UNNEST(labels) AS labels
  ON labels.key = "k8s-label/workflows.argoproj.io/workflow"
WHERE
  project.id = "moz-fx-mfouterbounds-prod-f98d"
  AND usage_start_time >= '2024-07-10'
GROUP BY
  invoice_day,
  run_id
ORDER BY
  flow_name DESC
