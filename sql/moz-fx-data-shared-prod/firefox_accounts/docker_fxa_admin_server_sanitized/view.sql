CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.docker_fxa_admin_server_sanitized`
AS
SELECT
  * REPLACE(
    STRUCT(
      resource.type,
      STRUCT(
        resource.labels.project_id,
        CAST(NULL AS STRING) AS pod_name,
        CAST(NULL AS STRING) AS container_name,
        CAST(NULL AS STRING) AS cluster_name,
        CAST(NULL AS STRING) AS namespace_name,
        CAST(NULL AS STRING) AS location,
        CAST(NULL AS STRING) AS zone,
        CAST(NULL AS STRING) AS instance_id
      ) AS labels
    ) AS resource,
    STRUCT(
      CAST(NULL AS STRING) AS k8s_pod_app_kubernetes_io_component,
      CAST(NULL AS STRING) AS k8s_pod_deployment,
      labels.compute_googleapis_com_resource_name,
      CAST(NULL AS STRING) AS k8s_pod_env_code,
      CAST(NULL AS STRING) AS k8s_pod_pod_template_hash,
      CAST(NULL AS STRING) AS k8s_pod_app_kubernetes_io_name,
      CAST(NULL AS STRING) AS k8s_pod_job_name,
      CAST(NULL AS STRING) AS k8s_pod_controller_uid,
      CAST(NULL AS STRING) AS k8s_pod_redis,
      labels.stack,
      labels.application,
      labels.env,
      labels.type
    ) AS labels,
    STRUCT(
      jsonPayload.timestamp,
      STRUCT(
        jsonPayload.fields.event,
        jsonPayload.fields.search_type
      ) AS fields
    ) AS jsonPayload
  )
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.docker_fxa_admin_server_sanitized_v1`
UNION ALL
SELECT
  * REPLACE(
    STRUCT(
      resource.type,
      STRUCT(
        resource.labels.project_id,
        resource.labels.pod_name,
        resource.labels.container_name,
        resource.labels.cluster_name,
        resource.labels.namespace_name,
        resource.labels.location,
        CAST(NULL AS STRING) AS zone,
        CAST(NULL AS STRING) AS instance_id
      ) AS labels
    ) AS resource,
    STRUCT(
      labels.k8s_pod_app_kubernetes_io_component,
      labels.k8s_pod_deployment,
      labels.compute_googleapis_com_resource_name,
      labels.k8s_pod_env_code,
      labels.k8s_pod_pod_template_hash,
      labels.k8s_pod_app_kubernetes_io_name,
      labels.k8s_pod_job_name,
      labels.k8s_pod_controller_uid,
      labels.k8s_pod_redis,
      CAST(NULL AS STRING) AS stack,
      CAST(NULL AS STRING) AS application,
      CAST(NULL AS STRING) AS env,
      CAST(NULL AS STRING) AS type
    ) AS labels,
    STRUCT(
      jsonPayload.timestamp,
      STRUCT(
        jsonPayload.fields.event,
        jsonPayload.fields.search_type
      ) AS fields
    ) AS jsonPayload
  )
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.docker_fxa_admin_server_sanitized_v2`
