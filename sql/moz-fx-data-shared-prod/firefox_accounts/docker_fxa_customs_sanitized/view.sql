CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.docker_fxa_customs_sanitized`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.docker_fxa_customs_sanitized_v1`
