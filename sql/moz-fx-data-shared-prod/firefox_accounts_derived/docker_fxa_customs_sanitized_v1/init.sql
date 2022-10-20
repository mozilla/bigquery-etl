-- Query for creating firefox_accounts_derived.docker_fxa_customs_sanitized_v1
CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod.firefox_accounts_derived.docker_fxa_customs_sanitized_v1`
PARTITION BY
  date
AS
SELECT
  DATE(timestamp) AS date,
  * REPLACE (
    (
      SELECT AS STRUCT
        jsonPayload.* REPLACE (SHA256(jsonPayload.email) AS email, SHA256(jsonPayload.ip) AS ip)
    ) AS jsonPayload
  )
FROM
  `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_customs*`
WHERE
  _TABLE_SUFFIX >= FORMAT_DATE('%y%m%d', "2020-01-01")
