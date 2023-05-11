CREATE TABLE IF NOT EXISTS
  fenix_derived.clients_yearly_v1
PARTITION BY
  submission_date
CLUSTER BY
  sample_id,
  client_id
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  CAST(NULL AS BYTES) AS days_seen_bytes,
  * EXCEPT (normalized_app_id),
FROM
  fenix.baseline_clients_daily
WHERE
  -- Output empty table and read no input rows
  FALSE
