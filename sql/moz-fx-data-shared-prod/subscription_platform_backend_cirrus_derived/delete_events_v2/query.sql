SELECT
  MIN(`timestamp`) AS submission_timestamp,
  jsonPayload.fields.nimbus_user_id AS nimbus_user_id,
FROM
  `moz-fx-fxa-prod.gke_fxa_prod_log.stderr`
WHERE
  (
    DATE(_PARTITIONTIME)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND DATE_ADD(@submission_date, INTERVAL 1 DAY)
  )
  AND DATE(`timestamp`) = @submission_date
  AND jsonPayload.type = 'glean.user.delete'
  AND jsonPayload.fields.nimbus_user_id IS NOT NULL
GROUP BY
  jsonPayload.fields.nimbus_user_id
