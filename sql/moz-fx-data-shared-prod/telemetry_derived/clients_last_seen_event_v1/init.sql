CREATE TABLE IF NOT EXISTS
  clients_last_seen_event_v1
PARTITION BY
  submission_date
CLUSTER BY
  sample_id
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  CAST(NULL AS INT64) AS days_logged_event_bits,
  CAST(NULL AS INT64) AS days_used_pictureinpicture_bits,
  CAST(NULL AS INT64) AS days_viewed_protection_report_bits,
  -- We make sure to delay * until the end so that as new columns are added
  -- to clients_daily_event we can add those columns in the same order to the end
  -- of this schema, which may be necessary for the daily join query between
  -- the two tables to validate.
  *
FROM
  clients_daily_event_v1
WHERE
  -- Output empty table and read no input rows
  FALSE
