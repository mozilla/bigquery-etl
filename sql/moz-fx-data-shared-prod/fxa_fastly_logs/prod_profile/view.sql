-- Generated via ./bqetl generate fxa_fastly_logs
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fxa_fastly_logs.prod_profile`
AS
SELECT
  *
FROM
  `moz-fx-fxa-prod.fxa_profile_prod_prod_fastly_cdn_logs.fastly`
