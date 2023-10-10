CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.docker_fxa_admin_server_sanitized`
AS
SELECT
  * EXCEPT(resource, labels, jsonPayload)
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.docker_fxa_admin_server_sanitized_v1`
UNION ALL

-- TODO, try alligning those fields as much as possible
SELECT
  * EXCEPT(resource, labels, jsonPayload)
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.docker_fxa_admin_server_sanitized_v2`
