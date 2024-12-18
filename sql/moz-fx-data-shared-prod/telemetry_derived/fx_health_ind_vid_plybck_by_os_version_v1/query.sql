SELECT
  DATE(submission_timestamp) AS submission_date,
  normalized_os_version,
  SUM(
    `moz-fx-data-shared-prod.udf.histogram_max_key_with_nonzero_value`(
      payload.processes.content.histograms.video_play_time_ms
    )
  ) / count(DISTINCT client_id) / 60000 AS play_time_ratio
FROM
  `moz-fx-data-shared-prod.telemetry.main_1pct`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND normalized_os = 'Windows'
GROUP BY
  DATE(submission_timestamp),
  normalized_os_version
