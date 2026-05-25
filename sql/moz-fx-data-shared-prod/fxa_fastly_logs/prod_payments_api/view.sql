-- Generated via ./bqetl generate fxa_fastly_logs
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fxa_fastly_logs.prod_payments_api`
AS
SELECT
  *
FROM
  `moz-fx-fxa-prod.fxa_payments_api_prod_prod_fastly_cdn_logs.fastly`
