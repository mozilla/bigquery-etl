WITH first_seen AS (
  SELECT
    submission_date,
    sample_id,
    client_id,
    first_seen_date,
    second_seen_date
  FROM
    telemetry.clients_last_seen
  WHERE
    days_since_seen = 0
    AND submission_date = @submission_date
),
daily_events AS (
  SELECT
    *
  FROM
    telemetry_derived.clients_daily_event_v1
  WHERE
    submission_date = @submission_date
),
crashes_daily AS (
  SELECT
    submission_date,
    sample_id,
    client_id,
    main_crash_count,
    content_crash_count,
    gpu_crash_count,
    rdd_crash_count,
    socket_crash_count,
    utility_crash_count,
    vr_crash_count,
  FROM
    telemetry.crashes_daily
  WHERE
    submission_date = @submission_date
)
SELECT
  *
FROM
  telemetry_derived.clients_daily_v6 AS cd
LEFT JOIN
  daily_events
  USING (submission_date, sample_id, client_id)
LEFT JOIN
  first_seen
  USING (submission_date, sample_id, client_id)
LEFT JOIN
  crashes_daily
  USING (submission_date, sample_id, client_id)
WHERE
  cd.submission_date = @submission_date
