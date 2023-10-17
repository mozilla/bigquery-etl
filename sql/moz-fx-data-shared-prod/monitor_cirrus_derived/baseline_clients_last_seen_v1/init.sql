-- Generated via bigquery_etl.glean_usage
CREATE TABLE IF NOT EXISTS
  `monitor_cirrus_derived.baseline_clients_last_seen_v1`
PARTITION BY
  submission_date
CLUSTER BY
  normalized_channel,
  sample_id
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  CAST(NULL AS INT64) AS days_seen_bits,
  CAST(NULL AS INT64) AS days_created_profile_bits,
  -- We make sure to delay * until the end so that as new columns are added
  -- to the daily table we can add those columns in the same order to the end
  -- of this schema, which may be necessary for the daily join query between
  -- the two tables to validate.
  *
FROM
  `monitor_cirrus_derived.baseline_clients_daily_v1`
WHERE
  -- Output empty table and read no input rows
  FALSE
