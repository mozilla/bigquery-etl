CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fxa_users_services_last_seen` AS
SELECT
  -- We cannot use UDFs in a view, so we paste the body of udf_bitpos(bits) literally here.
  CAST(SAFE.LOG(days_seen_bits & -days_seen_bits, 2) AS INT64) AS days_since_seen,
  CAST(SAFE.LOG(days_seen_in_tier1_country_bits & -days_seen_in_tier1_country_bits, 2) AS INT64) AS days_since_seen_in_tier1_country,
  CAST(SAFE.LOG(days_registered_bits & -days_registered_bits, 2) AS INT64) AS days_since_registered,
  * REPLACE(cn.code AS country),
  fxa.country AS country_name
FROM
  `moz-fx-data-shared-prod.telemetry_derived.fxa_users_services_last_seen_v1` AS fxa
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_names_v1` AS cn
  ON cn.name = fxa.country
