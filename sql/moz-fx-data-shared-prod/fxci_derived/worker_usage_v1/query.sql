WITH azure_billing AS (
  SELECT
    daily_load.subscriptionName,
    daily_load.`date`,
    daily_load.ResourceId,
    daily_load.resourceLocation,
    daily_load.quantity,
    daily_load.chargeType,
    daily_load.meterCategory,
    daily_load.unitOfMeasure
  FROM
    `moz-fx-data-shared-prod.azure_billing_syndicate.fxci_daily_actual_load` AS daily_load
  INNER JOIN
    `moz-fx-data-shared-prod.azure_billing_syndicate.fxci_daily_actual_lookup` AS daily_lookup
    USING (sourceDataId)
  WHERE
    daily_load.`date` = @submission_date
  UNION ALL
  SELECT
    daily_load.subscriptionName,
    daily_load.`date`,
    daily_load.ResourceId,
    daily_load.resourceLocation,
    daily_load.quantity,
    daily_load.chargeType,
    daily_load.meterCategory,
    daily_load.unitOfMeasure
  FROM
    `moz-fx-data-shared-prod.azure_billing_syndicate.taskcluster_daily_actual_load` AS daily_load
  INNER JOIN
    `moz-fx-data-shared-prod.azure_billing_syndicate.taskcluster_daily_actual_lookup` AS daily_lookup
    USING (sourceDataId)
  WHERE
    daily_load.`date` = @submission_date
)
SELECT
  "gcp" AS cloud_provider,
  project,
  zone,
  instance_id,
  submission_date AS usage_start_date,
  SUM(uptime) AS uptime,
  "gcp_monitoring_instance_uptime" AS attribution_method
FROM
  `moz-fx-data-shared-prod.fxci_derived.worker_metrics_v1`
WHERE
  submission_date = @submission_date
GROUP BY
  project,
  zone,
  instance_id,
  usage_start_date
UNION ALL
SELECT
  "azure" AS cloud_provider,
  subscriptionName AS project,
  resourceLocation AS zone,
  REGEXP_EXTRACT(ResourceId, r"(?i)/virtualmachines/([^/]+)$") AS instance_id,
  DATE(`date`) AS usage_start_date,
  SUM(quantity * 3600) AS uptime,
  "azure_billing_vm_quantity" AS attribution_method
FROM
  azure_billing
WHERE
  REGEXP_CONTAINS(ResourceId, r"(?i)/virtualmachines/vm-")
  AND chargeType = "Usage"
  AND meterCategory = "Virtual Machines"
  AND unitOfMeasure = "1 Hour"
GROUP BY
  project,
  zone,
  instance_id,
  usage_start_date
