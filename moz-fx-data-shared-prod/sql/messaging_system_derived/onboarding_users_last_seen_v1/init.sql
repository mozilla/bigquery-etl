CREATE TABLE
  onboarding_users_last_seen_v1
PARTITION BY
  submission_date
CLUSTER BY
  release_channel
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  CAST(NULL AS INT64) AS days_seen_bits,
-- We make sure to delay * until the end so that as new columns are added
-- to the daily table, we can add those columns in the same order to the end
-- of this schema, which may be necessary for the daily join query between
-- the two tables to validate.
  *
FROM
  onboarding_users_daily_v1
WHERE
-- Output empty table and read no input rows
  FALSE
