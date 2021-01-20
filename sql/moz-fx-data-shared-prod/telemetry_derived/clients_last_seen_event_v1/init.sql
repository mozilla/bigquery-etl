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
  *
FROM
  clients_daily_event_v1
WHERE
  -- Output empty table and read no input rows
  FALSE
