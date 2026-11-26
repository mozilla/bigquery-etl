CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.llm_proxy_fastly_logs.prod`
AS
SELECT
  *
FROM
  `moz-fx-llm-proxy-prod-b0b8.llm_proxy_prod_prod_fastly_cdn_logs.fastly`
