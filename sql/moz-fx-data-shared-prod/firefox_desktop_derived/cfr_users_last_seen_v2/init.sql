CREATE TABLE
  cfr_users_last_seen_v2
PARTITION BY
  submission_date
CLUSTER BY
  release_channel
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  CAST(NULL AS INT64) AS days_seen_bits,
  CAST(NULL AS INT64) AS days_seen_whats_new_bits,
  CAST(NULL AS DATE) AS submission_date,
  CAST(NULL AS STRING) AS impression_id,
  CAST(NULL AS STRING) AS client_id,
  CAST(NULL AS BOOLEAN) AS seen_whats_new,
  CAST(NULL AS STRING) AS release_channel,
  CAST(NULL AS STRING) AS locale,
  CAST(NULL AS STRING) AS country,
  CAST(NULL AS STRING) AS version
