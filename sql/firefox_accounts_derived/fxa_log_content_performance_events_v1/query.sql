SELECT
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
-- timestamp, -- This is a static value in 1970 for all records
  receiveTimestamp
FROM
  `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_content_19700101`
WHERE
  REGEXP_CONTAINS(jsonPayload.event, r"^flow\.performance.[\w-]+")
  AND DATE(receiveTimestamp) = @submission_timestamp
