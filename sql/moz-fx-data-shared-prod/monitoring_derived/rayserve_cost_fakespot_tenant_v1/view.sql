with cost_data as (
SELECT
  cost + (IFNULL((SELECT SUM(c.amount)
                  FROM UNNEST(credits) c), 0))
    AS total_cost,
  #labels,
  DATE_TRUNC(usage_start_time, DAY) AS invoice_day,
(
 SELECT
  value
 FROM
  UNNEST(labels)
 WHERE
  KEY = "k8s-namespace"
) AS k8s_namespace,

(
  SELECT
   value
  FROM
   UNNEST(labels)
  WHERE
   KEY = "k8s-label/app.kubernetes.io/created-by"
 ) AS k8s_label_akio_createdBy,

FROM
moz-fx-data-shared-prod.billing_syndicate.gcp_billing_export_resource_v1_01E7D5_97288E_E2EBA0

WHERE
project.id = "moz-fx-dataservices-high-nonpr" #moz-fx-dataservices-high-nonpr
AND DATE(usage_start_time) >= '2024-01-01'
AND service.description = "Compute Engine"
),
 daily_cost_data_per_kuberay_workload as (
  select SUM(total_cost) as daily_cost_per_kuberay_workload, invoice_day, k8s_namespace, k8s_label_akio_createdBy,
  from cost_data
  where k8s_namespace = "fakespot-ml-stage" AND k8s_label_akio_createdBy = "kuberay-operator"
  group by
  invoice_day, k8s_namespace, k8s_label_akio_createdBy
  order by invoice_day
)

SELECT SUM(daily_cost_per_kuberay_workload)
From daily_cost_data_per_kuberay_workload


#select COUNT(total), COUNT(k8s_namespace), COUNT(total), COUNT(k8s_namespace), COUNT(goog_k8s_namespace), COUNT(k8s_namespace_labels_meta_name) from data
/*select labels from data where k8s_namespace IN (#'kube:unallocated'
     #'goog-k8s-unsupported-sku',
     #'goog-k8s-unknown',
     'kube-system'
     #'kube:system-overhead'
     )*/
#select COUNT(total), COUNT(k8s_namespace), COUNT(goog_k8s_namespace), COUNT(k8s_namespace_labels_meta_name) from data where k8s_namespace = "fakespot-ml-stage"
#select labels from data
#select total from data
/*
(
 SELECT
  value
 FROM
  UNNEST(labels)
 WHERE
  KEY = "k8s-namespace-labels/kubernetes.io/metadata.name"
) AS k8s_namespace_labels_meta_name,*/