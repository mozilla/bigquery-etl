-- Generated via ./bqetl generate fxa_fastly_logs
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fxa_fastly_logs.prod_api_accounts`
AS
SELECT
  *
FROM
  `moz-fx-fxa-prod.fxa_api_accounts_prod_prod_fastly_cdn_logs.fastly`
