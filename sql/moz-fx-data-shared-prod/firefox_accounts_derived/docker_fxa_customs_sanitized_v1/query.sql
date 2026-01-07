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
  `moz-fx-data-shared-prod.firefox_accounts_prod_logs_syndicate.docker_fxa_customs`
WHERE
  {% if is_init() %}
    DATE(`timestamp`) = "2020-01-01"
  {% else %}
    DATE(`timestamp`) = @submission_date
  {% endif %}
