-- Query for firefox_accounts_derived.docker_fxa_customs_sanitized_v2
SELECT
  DATE(@submission_date) AS `date`,
  * REPLACE (
    (
      SELECT AS STRUCT
        jsonPayload.* REPLACE (SHA256(jsonPayload.email) AS email, SHA256(jsonPayload.ip) AS ip)
    ) AS jsonPayload
  )
FROM
  `moz-fx-fxa-prod.gke_fxa_prod_log.stderr`
WHERE
  DATE(_PARTITIONTIME) BETWEEN
    DATE_SUB(@submission_date, INTERVAL 1 DAY) AND @submission_date
  AND DATE(`timestamp`) = @submission_date
  -- v2 consumes events from GCP based fxa service,
  -- the date indicates when the migration to GCP started.
  AND DATE(`timestamp`) >= "2023-09-07"
  AND labels.k8s_pod_deployment = "customs"
  AND resource.labels.container_name = "customs"
