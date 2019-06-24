
--
CREATE TABLE
  clients_last_seen_raw_v1
PARTITION BY
  submission_date
CLUSTER BY
  sample_id,
  client_id AS
OPTIONS
  ( require_partition_filter=TRUE) AS
SELECT
  CAST(NULL AS DATE) AS submission_date,
  0 AS days_seen_bits,
  0 AS days_visited_5_uri_bits,
  0 AS days_opened_dev_tools_bits,
  0 AS days_created_profile_bits,
  [STRUCT("" AS experiment, "" AS branch, 0 AS bits)] AS days_seen_in_experiment,
  -- We make sure to delay * until the end so that as new columns are added
  -- to clients_daily, we can add those columns in the same order to the end
  -- of this schema, which may be necessary for the daily join query between
  -- the two tables to validate.
  * EXCEPT (submission_date_s3)
FROM
  clients_daily_v6
WHERE
  -- Output empty table and read no input rows
  FALSE
