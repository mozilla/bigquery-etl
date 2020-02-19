CREATE TABLE
  `moz-fx-data-shared-prod.org_mozilla_firefox_derived.clients_last_seen_v1`
PARTITION BY
  submission_date
CLUSTER BY
  normalized_channel,
  sample_id
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  CAST(NULL AS DATE) AS submission_date,
  client_id,
  sample_id,
  baseline.normalized_channel,
  0 AS days_seen_bits,
-- We make sure to delay * until the end so that as new columns are added
-- to clients_daily, we can add those columns in the same order to the end
-- of this schema, which may be necessary for the daily join query between
-- the two tables to validate.
  (
    SELECT AS STRUCT
      baseline.* EXCEPT (submission_date, client_id, sample_id, normalized_channel)
  ) AS baseline,
  (SELECT AS STRUCT metrics.* EXCEPT (submission_date, client_id, sample_id)) AS metrics,
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_derived.baseline_daily_v1` AS baseline
LEFT JOIN
  `moz-fx-data-shared-prod.org_mozilla_firefox_derived.metrics_daily_v1` AS metrics
USING
  (client_id, sample_id)
WHERE
-- Output empty table and read no input rows
  FALSE
