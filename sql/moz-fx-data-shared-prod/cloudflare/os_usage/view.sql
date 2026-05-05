CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.cloudflare.os_usage`
AS
SELECT
  os_usage.*,
  country_codes.location_name,
  country_codes.region_name
FROM
  `moz-fx-data-shared-prod.cloudflare_derived.os_usage_v1` os_usage
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
  ON os_usage.location = country_codes.code
