-- shredder requires this table contain client_id and be partitioned by submission_timestamp
SELECT
  client_info.client_id,
  -- if @submission_date is null this is a backfill, use CURRENT_TIMESTAMP - 1 DAY to ensure results
  -- will be serviced by the next shredder run and also not in a partition that will be overwritten
  -- by the next airflow run, otherwise any value in @submission_date will suffice.
  IF(
    CAST(@submission_date AS DATE) IS NULL,
    CURRENT_TIMESTAMP - INTERVAL 1 DAY,
    ANY_VALUE(submission_timestamp)
  ) AS submission_timestamp,
FROM
  `moz-fx-data-shared-prod`.org_mozilla_focus_beta_stable.events_v1
CROSS JOIN
  UNNEST(events) AS event
WHERE
  submission_timestamp > "2010-01-01"
  AND (
    CAST(@submission_date AS DATE) IS NULL
    OR CAST(@submission_date AS DATE) = DATE(submission_timestamp)
  )
  AND event.category = "privacy_settings"
  AND event.name = "telemetry_setting_changed"
  AND mozfun.map.get_key(event.extra, "is_enabled") = "false"
GROUP BY
  client_id
