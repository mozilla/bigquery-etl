SELECT
  DATE(submission_timestamp) AS submission_date,
  normalized_os,
  SUM(
    `moz-fx-data-shared-prod.udf.histogram_max_key_with_nonzero_value`(
      payload.processes.content.histograms.video_play_time_ms
    )
  ) / count(DISTINCT client_id) / 60000 AS play_time_ratio
FROM
  `moz-fx-data-shared-prod.telemetry.main`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND normalized_os IN ('Windows', 'Linux', 'Mac')
  AND sample_id = 42
GROUP BY
  DATE(submission_timestamp),
  normalized_os
