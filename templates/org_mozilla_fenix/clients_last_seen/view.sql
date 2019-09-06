CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_fenix.clients_last_seen` AS
SELECT
  -- Include date_last_seen for compatibility with existing queries.
  DATE_SUB(submission_date, INTERVAL days_since_seen DAY) AS date_last_seen,
  -- We cannot use UDFs in a view, so we paste the body of udf_bitpos(bits) literally here.
  CAST(SAFE.LOG(days_seen_bits & -days_seen_bits, 2) AS INT64) AS days_since_seen,
  CAST(SAFE.LOG(days_created_profile_bits & -days_created_profile_bits, 2) AS INT64) AS days_since_created_profile,
  *
FROM
  `moz-fx-data-derived-datasets.org_mozilla_fenix.clients_last_seen_v1`
