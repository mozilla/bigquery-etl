CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.cloudflare.browser_usage`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.cloudflare_derived.browser_usage_v1`
