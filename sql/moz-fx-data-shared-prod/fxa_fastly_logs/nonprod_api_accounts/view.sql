-- Generated via ./bqetl generate fxa_fastly_logs
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fxa_fastly_logs.nonprod_api_accounts`
AS
SELECT
  *
FROM
  `moz-fx-fxa-nonprod.fxa_api_accounts_nonprod_stage_fastly_cdn_logs.fastly`
