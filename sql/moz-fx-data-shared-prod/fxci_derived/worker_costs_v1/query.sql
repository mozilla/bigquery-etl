SELECT
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
