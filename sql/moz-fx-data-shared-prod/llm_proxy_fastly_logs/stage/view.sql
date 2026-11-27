CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.llm_proxy_fastly_logs.stage`
AS
SELECT
  *
FROM
  `moz-fx-llm-proxy-nonprod-fd0e.llm_proxy_nonprod_stage_fastly_cdn_logs.fastly`
