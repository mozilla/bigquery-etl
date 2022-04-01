WITH fxa_content_auth_stdout_events_live AS (
  SELECT
    PARSE_DATE('%y%m%d', _TABLE_SUFFIX) AS submission_date,
    JSON_VALUE(jsonPayload.fields.user_properties, '$.flow_id') AS flow_id,
    `timestamp`,
    TO_HEX(SHA256(jsonPayload.fields.user_id)) AS fxa_uid,
  FROM
    `moz-fx-fxa-prod-0712.fxa_stage_logs.docker_fxa_auth_20*`
  WHERE
    jsonPayload.type = 'amplitudeEvent'
    AND jsonPayload.fields.event_type IS NOT NULL
    AND jsonPayload.fields.user_id IS NOT NULL
  UNION ALL
  SELECT
    PARSE_DATE('%y%m%d', _TABLE_SUFFIX) AS submission_date,
    JSON_VALUE(jsonPayload.fields.user_properties, '$.flow_id') AS flow_id,
    `timestamp`,
    TO_HEX(SHA256(jsonPayload.fields.user_id)) AS fxa_uid,
  FROM
    `moz-fx-fxa-prod-0712.fxa_stage_logs.docker_fxa_content_20*`
  WHERE
    jsonPayload.type = 'amplitudeEvent'
    AND jsonPayload.fields.event_type IS NOT NULL
  UNION ALL
  SELECT
    PARSE_DATE('%y%m%d', _TABLE_SUFFIX) AS submission_date,
    JSON_VALUE(jsonPayload.fields.user_properties, '$.flow_id') AS flow_id,
    `timestamp`,
    TO_HEX(SHA256(jsonPayload.fields.user_id)) AS fxa_uid,
  FROM
    `moz-fx-fxa-prod-0712.fxa_stage_logs.stdout_20*`
  WHERE
    jsonPayload.type = 'amplitudeEvent'
    AND jsonPayload.fields.event_type IS NOT NULL
)
SELECT
  submission_date,
  flow_id,
  MIN(`timestamp`) AS flow_started,
  ARRAY_AGG(
    IF(fxa_uid IS NULL, NULL, STRUCT(fxa_uid, `timestamp` AS fxa_uid_timestamp)) IGNORE NULLS
    ORDER BY
      `timestamp` DESC
    LIMIT
      1
  )[SAFE_OFFSET(0)].*,
FROM
  fxa_content_auth_stdout_events_live
WHERE
  submission_date = @submission_date
  AND flow_id IS NOT NULL
GROUP BY
  submission_date,
  flow_id
