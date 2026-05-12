WITH azure_billing AS (
  SELECT
    daily_load.subscriptionName,
    daily_load.`date`,
    daily_load.ResourceId,
    daily_load.resourceLocation,
    daily_load.costInBillingCurrency,
    daily_load.chargeType
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
    daily_load.costInBillingCurrency,
    daily_load.chargeType
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
  project.id AS project,
  IF(
    resource.name LIKE "%/instances/%",
    REGEXP_EXTRACT(resource.name, "/instances/(.+)$"),
    resource.name
  ) AS name,
  REGEXP_EXTRACT(resource.global_name, "/zones/([^/]+)") AS zone,
  REGEXP_EXTRACT(resource.global_name, "/instances/([^/]+)") AS instance_id,
  DATE(usage_start_time) AS usage_start_date,
  SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) AS total_cost
FROM
  `moz-fx-data-shared-prod.billing_syndicate.gcp_billing_export_resource_v1_01E7D5_97288E_E2EBA0`
WHERE
  DATE(usage_start_time) = @submission_date
  AND (
    project.id = "fxci-production-level3-workers"
    OR project.id = "fxci-production-level1-workers"
  )
  AND resource.global_name LIKE "%/instances/%"
GROUP BY
  name,
  project,
  zone,
  instance_id,
  usage_start_date
UNION ALL
SELECT
  "azure" AS cloud_provider,
  subscriptionName AS project,
  REGEXP_EXTRACT(ResourceId, r"(?i)/virtualmachines/([^/]+)$") AS name,
  resourceLocation AS zone,
  REGEXP_EXTRACT(ResourceId, r"(?i)/virtualmachines/([^/]+)$") AS instance_id,
  DATE(`date`) AS usage_start_date,
  SUM(costInBillingCurrency) AS total_cost
FROM
  azure_billing
WHERE
  REGEXP_CONTAINS(ResourceId, r"(?i)/virtualmachines/vm-")
  AND chargeType = "Usage"
GROUP BY
  name,
  project,
  zone,
  instance_id,
  usage_start_date
