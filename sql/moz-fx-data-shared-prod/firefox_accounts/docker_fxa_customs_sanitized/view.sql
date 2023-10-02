CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.docker_fxa_customs_sanitized`
AS
SELECT
  `date`,
  logName,
  STRUCT(
    resource.type,
    STRUCT(
      resource.labels.instance_id,
      resource.labels.zone,
      resource.labels.project_id,
      CAST(NULL AS STRING) AS cluster_name,
      CAST(NULL AS STRING) AS namespace_name,
      CAST(NULL AS STRING) AS container_name,
      CAST(NULL AS STRING) AS pod_name,
      CAST(NULL AS STRING) AS location
    ) AS labels
  ) AS resource,
  textPayload,
  STRUCT(
    jsonPayload.block,
    jsonPayload.op,
    jsonPayload.action,
    jsonPayload.errno,
    jsonPayload.foundin
  ) AS jsonPayload,
  `timestamp`,
  receiveTimestamp,
  severity,
  insertId,
  httpRequest,
  STRUCT(
    labels.compute_googleapis_com_resource_name,
    CAST(NULL AS STRING) AS k8s_pod_deployment,
    CAST(NULL AS STRING) AS k8s_pod_app_kubernetes_io_component,
    CAST(NULL AS STRING) AS k8s_pod_env_code,
    CAST(NULL AS STRING) AS k8s_pod_pod_template_hash,
    CAST(NULL AS STRING) AS k8s_pod_app_kubernetes_io_name,
    CAST(NULL AS STRING) AS k8s_pod_app_kubernetes_io_instance
  ) AS labels,
  operation,
  trace,
  spanId,
  traceSampled,
  sourceLocation,
  split,
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.docker_fxa_customs_sanitized_v1`
  -- this filter indicates up until when the source table is expected to contains events
  -- due to AWS to GCP migration, after this date we expect the v2 to contain all the events from GCP.
WHERE
  `date` < "2023-09-28"
UNION ALL
SELECT
  `date`,
  logName,
  STRUCT(
    resource.type,
    STRUCT(
      CAST(NULL AS STRING) AS instance_id,
      CAST(NULL AS STRING) AS zone,
      resource.labels.project_id,
      resource.labels.cluster_name,
      resource.labels.namespace_name,
      resource.labels.container_name,
      resource.labels.pod_name,
      resource.labels.location
    ) AS labels
  ) AS resource,
  textPayload,
  STRUCT(
    jsonPayload.block,
    jsonPayload.op,
    jsonPayload.action,
    jsonPayload.errno,
    jsonPayload.foundin
  ) AS jsonPayload,
  `timestamp`,
  receiveTimestamp,
  severity,
  insertId,
  httpRequest,
  STRUCT(
    labels.compute_googleapis_com_resource_name,
    labels.k8s_pod_deployment,
    labels.k8s_pod_app_kubernetes_io_component,
    labels.k8s_pod_env_code,
    labels.k8s_pod_pod_template_hash,
    labels.k8s_pod_app_kubernetes_io_name,
    labels.k8s_pod_app_kubernetes_io_instance
  ) AS labels,
  operation,
  trace,
  spanId,
  traceSampled,
  sourceLocation,
  split,
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.docker_fxa_customs_sanitized_v2`
  -- this filter indicates from when the source table contains events
  -- due to AWS to GCP migration
WHERE
  `date` >= "2023-09-07"
