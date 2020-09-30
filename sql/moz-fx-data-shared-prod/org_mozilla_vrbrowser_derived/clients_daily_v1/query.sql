SELECT
  baseline.submission_date,
  baseline.client_id,
  baseline.sample_id,
  'Firefox Reality' AS app_name,
  baseline.first_run_date,
  baseline.durations,
  baseline.os,
  baseline.os_version,
  baseline.locale,
  baseline.country,
  baseline.city,
  baseline.device_manufacturer,
  baseline.device_model,
  baseline.app_build,
  baseline.normalized_channel,
  baseline.architecture,
  baseline.app_display_version,
  metrics.distribution_channel_name
FROM
  baseline_daily_v1 AS baseline
LEFT JOIN
  metrics_daily_v1 AS metrics
USING
  (submission_date, client_id)
WHERE
  -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
  (@submission_date IS NULL OR @submission_date = submission_date)
