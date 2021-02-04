CREATE TABLE
  clients_last_seen_v1
PARTITION BY
  submission_date
CLUSTER BY
  normalized_channel,
  sample_id
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  CAST(NULL AS DATE) AS first_seen_date,
  CAST(NULL AS DATE) AS second_seen_date,
  CAST(NULL AS INT64) AS days_seen_bits,
  CAST(NULL AS INT64) AS days_visited_1_uri_bits,
  CAST(NULL AS INT64) AS days_visited_5_uri_bits,
  CAST(NULL AS INT64) AS days_visited_10_uri_bits,
  CAST(NULL AS INT64) AS days_had_8_active_ticks_bits,
  CAST(NULL AS INT64) AS days_opened_dev_tools_bits,
  CAST(NULL AS INT64) AS days_interacted_bits,
  CAST(NULL AS INT64) AS days_created_profile_bits,
  ARRAY<STRUCT<experiment STRING, branch STRING, bits INT64>>[] AS days_seen_in_experiment,
  -- We make sure to delay * until the end so that as new columns are added
  -- to clients_daily, we can add those columns in the same order to the end
  -- of this schema, which may be necessary for the daily join query between
  -- the two tables to validate.
  *
FROM
  clients_daily_v6
WHERE
  -- Output empty table and read no input rows
  FALSE
