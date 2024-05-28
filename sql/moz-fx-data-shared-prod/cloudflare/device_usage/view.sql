CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.cloudflare.device_usage`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.cloudflare_derived.device_usage_v1`
