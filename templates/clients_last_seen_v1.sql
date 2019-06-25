CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.clients_last_seen_v1` AS
SELECT
  -- We cannot use UDFs in a view, so we paste the body of udf_bitpos(bits) literally here.
  CAST(SAFE.LOG(days_seen_bits & -days_seen_bits, 2) AS INT64) AS days_since_seen,
  CAST(SAFE.LOG(days_visited_5_uri_bits & -days_visited_5_uri_bits, 2) AS INT64) AS days_since_visited_5_uri,
  CAST(SAFE.LOG(days_opened_dev_tools_bits & -days_opened_dev_tools_bits, 2) AS INT64) AS days_since_opened_dev_tools,
  CAST(SAFE.LOG(days_created_profile_bits & -days_created_profile_bits, 2) AS INT64) AS days_since_created_profile,
  * REPLACE ( --
    ARRAY(
    SELECT
      AS STRUCT *,
      CAST(SAFE.LOG(bits & -bits, 2) AS INT64) AS days_since_seen
    FROM
      UNNEST(days_seen_in_experiment)) AS days_seen_in_experiment)
FROM
  `moz-fx-data-derived-datasets.telemetry.clients_last_seen_raw_v1`
