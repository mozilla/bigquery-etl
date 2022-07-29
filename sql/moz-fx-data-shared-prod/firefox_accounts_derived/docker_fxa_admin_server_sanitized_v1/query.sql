-- Query for firefox_accounts_derived.docker_fxa_admin_server_sanitized_v1
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
  `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_admin_server_*`
WHERE
  _TABLE_SUFFIX = FORMAT_DATE('%y%m%d', @submission_date)
