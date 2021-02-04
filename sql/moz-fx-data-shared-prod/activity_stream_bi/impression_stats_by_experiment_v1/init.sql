CREATE OR REPLACE TABLE
  activity_stream_bi.impression_stats_by_experiment_v1
PARTITION BY
  DATE(submission_timestamp)
CLUSTER BY
  experiment_id
OPTIONS
  (
    require_partition_filter = TRUE, --
    partition_expiration_days = 180
  )
AS
SELECT
  submission_timestamp,
  experiment.key AS experiment_id,
  experiment.value.branch AS experiment_branch,
  client_id,
  blocked,
  clicks,
  impressions,
  position,
  source,
  tile_id,
  user_prefs,
FROM
  activity_stream_bi.impression_stats_flat_v1
CROSS JOIN
  UNNEST(experiments) AS experiment
WHERE
  DATE(submission_timestamp) >= DATE_SUB(CURRENT_DATE, INTERVAL 180 DAY)
