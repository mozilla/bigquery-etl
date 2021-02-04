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
  DATE(submission_timestamp) = @submission_date
