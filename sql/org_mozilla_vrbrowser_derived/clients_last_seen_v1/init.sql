CREATE TABLE
  `moz-fx-data-shared-prod.org_mozilla_vrbrowser_derived.clients_last_seen_v1`
PARTITION BY
  submission_date
CLUSTER BY
  app_name,
  os,
  sample_id
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  CAST(NULL AS DATE) AS submission_date,
  0 AS days_seen_bits,
  0 AS days_created_profile_bits,
  -- We make sure to delay * until the end so that as new columns are added
  -- to clients_daily, we can add those columns in the same order to the end
  -- of this schema, which may be necessary for the daily join query between
  -- the two tables to validate.
  * EXCEPT (submission_date)
FROM
  `moz-fx-data-shared-prod.org_mozilla_vrbrowser_derived.clients_daily_v1`
WHERE
  -- Output empty table and read no input rows
  FALSE
