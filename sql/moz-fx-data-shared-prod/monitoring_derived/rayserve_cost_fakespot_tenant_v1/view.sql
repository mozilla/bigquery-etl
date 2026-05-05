CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring_derived.rayserve_cost_fakespot_tenant_v1`
AS
WITH cost_data AS (
  SELECT
    cost + (IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) AS total_cost,
    DATE_TRUNC(usage_start_time, DAY) AS invoice_day,
    (SELECT value FROM UNNEST(labels) WHERE KEY = "k8s-namespace") AS k8s_namespace,
    (
      SELECT
        value
      FROM
        UNNEST(labels)
      WHERE
        -- the label to identify the kuberay created ray serve workloads
        KEY = "k8s-label/app.kubernetes.io/created-by"
    ) AS k8s_label_akio_createdBy,
  FROM
    `moz-fx-data-shared-prod.billing_syndicate.gcp_billing_export_resource_v1_01E7D5_97288E_E2EBA0`
  WHERE
    project.id = "moz-fx-dataservices-high-nonpr"
    AND DATE(usage_start_time) >= '2024-01-01'
    AND service.description = "Compute Engine"
),
daily_cost_data_per_kuberay_workload AS (
  SELECT
    SUM(total_cost) AS daily_cost_per_kuberay_workload,
    invoice_day,
    k8s_namespace,
    k8s_label_akio_createdBy,
  FROM
    cost_data
  WHERE
    k8s_namespace = "fakespot-ml-stage"
    AND k8s_label_akio_createdBy = "kuberay-operator"
  GROUP BY
    invoice_day,
    k8s_namespace,
    k8s_label_akio_createdBy
  ORDER BY
    invoice_day
)
SELECT
  *
FROM
  daily_cost_data_per_kuberay_workload
