-- Generated via bigquery_etl.glean_usage
CREATE TABLE IF NOT EXISTS
  `org_mozilla_ios_firefox_derived.baseline_clients_first_seen_v1`
PARTITION BY
  first_seen_date
CLUSTER BY
  sample_id,
  submission_date
OPTIONS
  (require_partition_filter = FALSE)
AS
WITH baseline AS (
  SELECT
    client_info.client_id,
    sample_id,
    DATE(MIN(submission_timestamp)) AS submission_date,
    DATE(MIN(submission_timestamp)) AS first_seen_date,
  FROM
    `org_mozilla_ios_firefox_stable.baseline_v1`
    -- initialize by looking over all of history
  WHERE
    DATE(submission_timestamp) > "2010-01-01"
  GROUP BY
    client_id,
    sample_id
)
SELECT
  *
FROM
  baseline
