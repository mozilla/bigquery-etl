with heartbeat as (
  SELECT
    client_id,
    payload.survey_id,
    SPLIT(payload.survey_id, '::')[OFFSET(1)] as shield_id
  FROM
     --moz-fx-data-shared-prod.telemetry_stable.heartbeat_v4 
    heartbeat_v4 
  WHERE
    date(submission_timestamp) = @submission_date
    AND payload.survey_id like 'hb-das%'
),

clients_daily as (
  SELECT *
  FROM
    --telemetry.clients_daily
    clients_daily
  WHERE
    submission_date = @submission_date
    AND normalized_channel = 'release'
),

heartbeat_joined as (
  SELECT *
  FROM
    --uploaded from a csv that was generated in this notebook:
    --https://dbc-caf9527b-e073.cloud.databricks.com/#notebook/234967/command/234975
    --
    --analysis.survey_gizmo_test_data
    survey_gizmo_test_data sg
  JOIN 
    heartbeat hb USING(shield_id)
)


SELECT cd.*, hb.* EXCEPT(client_id, shield_id)
FROM
  heartbeat_joined hb
JOIN clients_daily cd
ON(hb.date = cd.submission_date AND hb.client_id = cd.client_id)