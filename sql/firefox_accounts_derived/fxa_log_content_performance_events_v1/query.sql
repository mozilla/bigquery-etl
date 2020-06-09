WITH base AS (
  SELECT
    receiveTimestamp AS `timestamp`,
    jsonPayload.event,
    jsonPayload.flow_time,
    jsonPayload.locale,
    jsonPayload.useragent,
    jsonPayload.country,
    jsonPayload.entrypoint,
    jsonPayload.flow_id,
    jsonPayload.region,
    jsonPayload.service,
    jsonPayload.utm_campaign,
    jsonPayload.utm_content,
    jsonPayload.utm_medium,
    jsonPayload.utm_source,
  FROM
    `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_content_19700101`
)
SELECT
  *
FROM
  base
WHERE
  REGEXP_CONTAINS(event, r"^flow\.performance.[\w-]+")
  AND DATE(timestamp) = @submission_timestamp
