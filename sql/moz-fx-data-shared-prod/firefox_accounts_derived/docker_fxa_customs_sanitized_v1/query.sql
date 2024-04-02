{% if is_init() %}
  SELECT
    DATE(timestamp) AS date,
    * REPLACE (
      (
        SELECT AS STRUCT
          jsonPayload.* REPLACE (SHA256(jsonPayload.email) AS email, SHA256(jsonPayload.ip) AS ip)
      ) AS jsonPayload
    )
  FROM
    `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_customs`
  WHERE
    DATE(`timestamp`) = "2020-01-01"
{% else %}
-- Query for firefox_accounts_derived.docker_fxa_customs_sanitized_v1
  SELECT
    @submission_date AS date,
    * REPLACE (
      (
        SELECT AS STRUCT
          jsonPayload.* REPLACE (SHA256(jsonPayload.email) AS email, SHA256(jsonPayload.ip) AS ip)
      ) AS jsonPayload
    )
  FROM
    `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_customs`
  WHERE
    DATE(`timestamp`) = @submission_date
{% endif %}
