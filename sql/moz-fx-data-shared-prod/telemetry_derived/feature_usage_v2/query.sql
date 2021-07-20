-- Generated via bigquery_etl.feature_usage
WITH main AS (
  SELECT
    client_id AS client_id,
    DATE(submission_timestamp) AS submission_date,
    LOGICAL_OR(COALESCE(environment.system.gfx.headless, FALSE)) AS is_headless,
    SUM(
      CAST(
        JSON_EXTRACT_SCALAR(
          payload.processes.content.histograms.video_encrypted_play_time_ms,
          '$.sum'
        ) AS int64
      )
    ) AS video_encrypted_play_time_ms,
  FROM
    telemetry.main_1pct
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id = 0
    AND normalized_channel = 'release'
  GROUP BY
    submission_date,
    client_id
),
user_type AS (
  SELECT
    client_id AS client_id,
    submission_date AS submission_date,
    activity_segments_v1 AS activity_segments_v1,
    app_version AS app_version,
  FROM
    telemetry.clients_last_seen
  WHERE
    submission_date = @submission_date
    AND sample_id = 0
    AND normalized_channel = 'release'
    AND days_since_seen = 0
),
all_features AS (
  SELECT
    *
  FROM
    main
  LEFT JOIN
    user_type
  USING
    (client_id, submission_date)
)
SELECT
  client_id,
  submission_date,
  IF('72' < app_version, COALESCE(is_headless, CAST(0 AS BOOL)), NULL) AS is_headless,
  IF(
    '70' < app_version,
    COALESCE(video_encrypted_play_time_ms, CAST(0 AS INT64)),
    NULL
  ) AS video_encrypted_play_time_ms,
  client_id,
  submission_date,
  activity_segments_v1,
  IF('42' < app_version, COALESCE(app_version, CAST(0 AS STRING)), NULL) AS app_version,
FROM
  all_features
