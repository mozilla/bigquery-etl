{{ header }}

CREATE OR REPLACE TABLE
  `moz-fx-data-backfill-8.{{ last_seen_table }}`
PARTITION BY submission_date
CLUSTER BY normalized_channel, sample_id
OPTIONS
  (require_partition_filter = TRUE)
AS

SELECT
    t.*,
    cfs.first_seen_date,
    t.submission_date = cfs.first_seen_date as is_new_profile
FROM
  `{{ last_seen_table }}` t
LEFT JOIN
  `{{ first_seen_table }}` cfs
USING
  (client_id)
WHERE
  t.submission_date > "2010-01-01"
