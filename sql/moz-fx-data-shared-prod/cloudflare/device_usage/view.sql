CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.cloudflare.device_usage`
AS
SELECT
  device_usg.*,
  country_codes.location_name,
  country_codes.region_name
FROM
  `moz-fx-data-shared-prod.cloudflare_derived.device_usage_v1` device_usg
LEFT JOIN
  (
    SELECT
      'ALL' AS code,
      'Global' AS location_name,
      'Global' AS region_name
    UNION ALL
    SELECT
      code,
      name AS location_name,
      region_name
    FROM
      `moz-fx-data-shared-prod.static.country_codes_v1`
  ) country_codes
  ON device_usg.location = country_codes.code
