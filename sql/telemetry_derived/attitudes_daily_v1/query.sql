WITH heartbeat AS (
  SELECT
    client_id,
    payload.survey_id,
    SPLIT(payload.survey_id, '::')[OFFSET(1)] AS shield_id
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.heartbeat_v4`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND payload.survey_id LIKE 'hb-das%'
),
clients_daily AS (
  SELECT
    *
  FROM
    telemetry.clients_daily
  WHERE
    submission_date = @submission_date
    AND normalized_channel = 'release'
),
heartbeat_joined AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.survey_gizmo_daily_attitudes` AS sg
  JOIN
    heartbeat
  USING
    (shield_id)
  WHERE
    sg.date = @submission_date
)
SELECT
  cd.*,
  hb.* EXCEPT (client_id, shield_id)
FROM
  heartbeat_joined AS hb
JOIN
  clients_daily AS cd
ON
  (hb.date = cd.submission_date AND hb.client_id = cd.client_id)
