CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.cloudflare.os_usage`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.cloudflare_derived.os_usage_v1`
