-- Azure billing sends a fresh file each day containing the whole month so far.
-- The *_lookup table points at the canonical sourceDataId per day; joining on it
-- avoids double-counting. Filtering ResourceId to /virtualMachines/ scopes to VM
-- compute, matching worker_costs_v1 which also excludes disk/NIC/IP.
WITH azure_billing AS (
  SELECT
    daily_load.subscriptionName,
    daily_load.`date`,
    daily_load.ResourceId,
    daily_load.resourceLocation,
    daily_load.costInBillingCurrency,
    daily_load.chargeType
  FROM
    `moz-fx-data-billing-prod-9147.azure_billing_raw.fxci_daily_actual_load` AS daily_load
  INNER JOIN
    `moz-fx-data-billing-prod-9147.azure_billing_raw.fxci_daily_actual_lookup` AS daily_lookup
    USING (sourceDataId)
  UNION ALL
  SELECT
    daily_load.subscriptionName,
    daily_load.`date`,
    daily_load.ResourceId,
    daily_load.resourceLocation,
    daily_load.costInBillingCurrency,
    daily_load.chargeType
  FROM
    `moz-fx-data-billing-prod-9147.azure_billing_raw.trusted_fxci_daily_actual_load` AS daily_load
  INNER JOIN
    `moz-fx-data-billing-prod-9147.azure_billing_raw.trusted_fxci_daily_actual_lookup` AS daily_lookup
    USING (sourceDataId)
)
SELECT
  subscriptionName AS project,
  REGEXP_EXTRACT(ResourceId, r"/virtualMachines/([^/]+)$") AS name,
  resourceLocation AS zone,
  REGEXP_EXTRACT(ResourceId, r"/virtualMachines/([^/]+)$") AS instance_id,
  DATE(`date`) AS usage_start_date,
  SUM(costInBillingCurrency) AS total_cost
FROM
  azure_billing
WHERE
  DATE(`date`) = @submission_date
  -- vm-* prefix excludes Packer image-build VMs (tk-*) which never run tasks
  AND ResourceId LIKE "%/virtualMachines/vm-%"
  -- defensive: exclude any Refund / Purchase rows that could sneak in under a vm-* ResourceId
  AND chargeType = "Usage"
GROUP BY
  name,
  project,
  zone,
  instance_id,
  usage_start_date
