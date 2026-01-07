SELECT
  @submission_date AS date,
  * REPLACE (
    (
      SELECT AS STRUCT
        jsonPayload.* REPLACE (
          (
            SELECT AS STRUCT
              jsonPayload.fields.* REPLACE (
                SHA256(jsonPayload.fields.user) AS user,
                SHA256(jsonPayload.fields.email) AS email
              )
          ) AS fields
        )
    ) AS jsonPayload
  )
FROM
  `moz-fx-fxa-prod.gke_fxa_prod_log.stdout`
WHERE
  DATE(`timestamp`) = @submission_date
  AND (
    DATE(_PARTITIONTIME)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND DATE_ADD(@submission_date, INTERVAL 1 DAY)
  )
  -- v2 consumes events from GCP based fxa service,
  -- the date indicates when the migration to GCP started.
  AND DATE(`timestamp`) >= "2023-09-07"
  AND labels.k8s_pod_deployment = "admin-backend"
  AND resource.labels.container_name = "admin-backend"
