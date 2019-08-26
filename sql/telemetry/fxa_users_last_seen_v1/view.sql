CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fxa_users_last_seen_v1` AS
SELECT
  -- We cannot use UDFs in a view, so we paste the body of udf_bitpos(bits) literally here.
  CAST(SAFE.LOG(days_seen_bits & -days_seen_bits, 2) AS INT64) AS days_since_seen,
  CAST(SAFE.LOG(days_seen_in_tier1_country_bits & -days_seen_in_tier1_country_bits, 2) AS INT64) AS days_since_seen_in_tier1_country,
  CAST(SAFE.LOG(days_registered_bits & -days_registered_bits, 2) AS INT64) AS days_since_registered,
  *
FROM
  `moz-fx-data-derived-datasets.telemetry.fxa_users_last_seen_raw_v1`
